/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}.
 */
public class AbstractPreemptableResourceCalculator {

  protected final CapacitySchedulerPreemptionContext context;
  protected final ResourceCalculator rc;
  protected boolean isReservedPreemptionCandidatesSelector;
  private Resource stepFactor;
  private boolean allowQueuesBalanceAfterAllQueuesSatisfied;

  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      double assigned1 = getIdealPctOfGuaranteed(tq1);
      double assigned2 = getIdealPctOfGuaranteed(tq2);

      return PriorityUtilizationQueueOrderingPolicy.compare(assigned1,
          assigned2, tq1.relativePriority, tq2.relativePriority);
    }

    // Calculates idealAssigned / guaranteed
    // TempQueues with 0 guarantees are always considered the most over
    // capacity and therefore considered last for resources.
    private double getIdealPctOfGuaranteed(TempQueuePerPartition q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(rc, clusterRes, q.getGuaranteed(),
          Resources.none())) {
        pctOver = Resources.divide(rc, clusterRes, q.idealAssigned,
            q.getGuaranteed());
      }
      return (pctOver);
    }
  }

  /**
   * PreemptableResourceCalculator constructor.
   *
   * @param preemptionContext context
   * @param isReservedPreemptionCandidatesSelector
   *          this will be set by different implementation of candidate
   *          selectors, please refer to TempQueuePerPartition#offer for
   *          details.
   * @param allowQueuesBalanceAfterAllQueuesSatisfied
   *          Should resources be preempted from an over-served queue when the
   *          requesting queues are all at or over their guarantees?
   *          An example is, there're 10 queues under root, guaranteed resource
   *          of them are all 10%.
   *          Assume there're two queues are using resources, queueA uses 10%
   *          queueB uses 90%. For all queues are guaranteed, but it's not fair
   *          for queueA.
   *          We wanna make this behavior can be configured. By default it is
   *          not allowed.
   *
   */
  public AbstractPreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    context = preemptionContext;
    rc = preemptionContext.getResourceCalculator();
    this.isReservedPreemptionCandidatesSelector =
        isReservedPreemptionCandidatesSelector;
    this.allowQueuesBalanceAfterAllQueuesSatisfied =
        allowQueuesBalanceAfterAllQueuesSatisfied;
    stepFactor = Resource.newInstance(0, 0);
    for (ResourceInformation ri : stepFactor.getResources()) {
      ri.setValue(1);
    }
  }

  /**
   * Given a set of queues compute the fix-point distribution of unassigned
   * resources among them. As pending request of a queue are exhausted, the
   * queue is removed from the set and remaining capacity redistributed among
   * remaining queues. The distribution is weighted based on guaranteed
   * capacity, unless asked to ignoreGuarantee, in which case resources are
   * distributed uniformly.
   *
   * @param totGuarant
   *          total guaranteed resource  父队列理想资源分配（最小资源配置）
   * @param qAlloc
   *          List of child queues       全部的子队列
   * @param unassigned
   *          Unassigned resource per queue  未被申请的资源
   * @param ignoreGuarantee
   *          ignore guarantee per queue.   是否忽略每个队列的最小资源配置
   */
  protected void computeFixpointAllocation(Resource totGuarant,
      Collection<TempQueuePerPartition> qAlloc, Resource unassigned,
      boolean ignoreGuarantee) {
    // Prior to assigning the unused resources, process each queue as follows:
    // If current > guaranteed, idealAssigned = guaranteed + untouchable extra
    // Else idealAssigned = current;
    // Subtract idealAssigned resources from unassigned.
    // If the queue has all of its needs met (that is, if
    // idealAssigned >= current + pending), remove it from consideration.
    // Sort queues from most under-guaranteed to most over-guaranteed.

    // 初始化优先级队列
    TQComparator tqComparator = new TQComparator(rc, totGuarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed = new PriorityQueue<>(10,
        tqComparator);

    //遍历同一个父队列的所有子队列
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext(); ) {
      //获取子队列的使用资源
      TempQueuePerPartition q = i.next();
      Resource used = q.getUsed();

      Resource initIdealAssigned;

      if (Resources.greaterThan(rc, totGuarant, used, q.getGuaranteed())) {

        initIdealAssigned = Resources.add(
                //分配资源 = min(理想资源<最小资源配置>，队列使用资源)
            Resources.componentwiseMin(q.getGuaranteed(), q.getUsed()),
            q.untouchableExtra);
      } else{
        //如果使用的资源小于最小分配资源，则该队列初始化资源分布为其使用资源
        initIdealAssigned = Resources.clone(used);
      }

      // perform initial assignment
      // 初始化该队列理想申请资源 = initIdealAssigned
      initIdealAssignment(totGuarant, q, initIdealAssigned);
      // 更新还未分配的资源
      Resources.subtractFrom(unassigned, q.idealAssigned);

      // If idealAssigned < (allocated + used + pending), q needs more
      // resources, so
      // add it to the list of underserved queues, ordered by need.
      // 如果理想分配资源 < (正在使用资源 + 阻塞等待资源)，说明还需要更多的资源，添加到orderedByNeed中，根据需求排序
      Resource curPlusPend = Resources.add(q.getUsed(), q.pending);
      if (Resources.lessThan(rc, totGuarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }

    // assign all cluster resources until no more demand, or no resources are
    // left
    // 继续分配资源，直到所有队列资源都满足，或者集群资源全部分配完成
    while (!orderedByNeed.isEmpty() && Resources.greaterThan(rc, totGuarant,
        unassigned, Resources.none())) {
      // we compute normalizedGuarantees capacity based on currently active
      // queues
      // 归一化队列资源
      resetCapacity(unassigned, orderedByNeed, ignoreGuarantee);

      // For each underserved queue (or set of queues if multiple are equally
      // underserved), offer its share of the unassigned resources based on its
      // normalized guarantee. After the offer, if the queue is not satisfied,
      // place it back in the ordered list of queues, recalculating its place
      // in the order of most under-guaranteed to most over-guaranteed. In this
      // way, the most underserved queue(s) are always given resources first.
      // 按照最缺资源的，优先分配资源。
      Collection<TempQueuePerPartition> underserved = getMostUnderservedQueues(
          orderedByNeed, tqComparator);

      // This value will be used in every round to calculate ideal allocation.
      // So make a copy to avoid it changed during calculation.
      // 还未分配完成的集群资源
      Resource dupUnassignedForTheRound = Resources.clone(unassigned);

      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        if (!rc.isAnyMajorResourceAboveZero(unassigned)) {
          break;
        }
        // 资源短缺的队列
        TempQueuePerPartition sub = i.next();

        // How much resource we offer to the queue (to increase its ideal_alloc
        // 队列可用资源 wQavail = 还未分配完成的集群资源 * 正则化的百分比，
        // 保证其计算的wQavail不能超过未分配的资源
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            dupUnassignedForTheRound,
            sub.normalizedGuarantee, this.stepFactor);
        wQavail = Resources.componentwiseMin(wQavail, unassigned);

        // 队列空闲资源
        Resource wQidle = sub.offer(wQavail, rc, totGuarant,
            isReservedPreemptionCandidatesSelector,
            allowQueuesBalanceAfterAllQueuesSatisfied);
        // 该队列此轮分配资源
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        //如果此轮分配的资源>0,则说明该队列资源还短缺
        if (Resources.greaterThan(rc, totGuarant, wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }
        //更新未分配资源 = 总的未分配资源 - 该队列此轮分配的资源
        Resources.subtractFrom(unassigned, wQdone);

        // Make sure unassigned is always larger than 0
        unassigned = Resources.componentwiseMax(unassigned, Resources.none());
      }
    }

    // Sometimes its possible that, all queues are properly served. So intra
    // queue preemption will not try for any preemption. How ever there are
    // chances that within a queue, there are some imbalances. Hence make sure
    // all queues are added to list.
    // 记录服务不足的队列
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
    }
  }


  /**
   * This method is visible to allow sub-classes to override the initialization
   * behavior.
   *
   * @param totGuarant total resources (useful for {@code ResourceCalculator}
   *          operations)
   * @param q the {@code TempQueuePerPartition} being initialized
   * @param initIdealAssigned the proposed initialization value.
   */
  protected void initIdealAssignment(Resource totGuarant,
      TempQueuePerPartition q, Resource initIdealAssigned) {
    q.idealAssigned = initIdealAssigned;
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues.
   *
   * @param clusterResource
   *          the total amount of resources in the cluster
   * @param queues
   *          the list of queues to consider
   * @param ignoreGuar
   *          ignore guarantee.
   */
  private void resetCapacity(Resource clusterResource,
      Collection<TempQueuePerPartition> queues, boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);
    int maxLength = ResourceUtils.getNumberOfKnownResourceTypes();

    //归一化资源
    if (ignoreGuar) {
      // 忽略最小资源配置，平均分配
      for (TempQueuePerPartition q : queues) {
        for (int i = 0; i < maxLength; i++) {
          q.normalizedGuarantee[i] = 1.0f / queues.size();
        }
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.getGuaranteed());
      }
      for (TempQueuePerPartition q : queues) {
        for (int i = 0; i < maxLength; i++) {
          ResourceInformation nResourceInformation = q.getGuaranteed()
              .getResourceInformation(i);
          ResourceInformation dResourceInformation = activeCap
              .getResourceInformation(i);

          long nValue = nResourceInformation.getValue();
          long dValue = UnitsConversionUtil.convert(
              dResourceInformation.getUnits(), nResourceInformation.getUnits(),
              dResourceInformation.getValue());
          if (dValue != 0) {
            q.normalizedGuarantee[i] = (float) nValue / dValue;
          }
        }
      }
    }
  }

  // Take the most underserved TempQueue (the one on the head). Collect and
  // return the list of all queues that have the same idealAssigned
  // percentage of guaranteed.
  private Collection<TempQueuePerPartition> getMostUnderservedQueues(
      PriorityQueue<TempQueuePerPartition> orderedByNeed,
      TQComparator tqComparator) {
    ArrayList<TempQueuePerPartition> underserved = new ArrayList<>();
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      underserved.add(q1);

      // Add underserved queues in order for later uses
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
      TempQueuePerPartition q2 = orderedByNeed.peek();
      // q1's pct of guaranteed won't be larger than q2's. If it's less, then
      // return what has already been collected. Otherwise, q1's pct of
      // guaranteed == that of q2, so add q2 to underserved list during the
      // next pass.
      if (q2 == null || tqComparator.compare(q1, q2) < 0) {
        if (null != q2) {
          context.addPartitionToUnderServedQueues(q2.queueName, q2.partition);
        }
        return underserved;
      }
    }
    return underserved;
  }
}