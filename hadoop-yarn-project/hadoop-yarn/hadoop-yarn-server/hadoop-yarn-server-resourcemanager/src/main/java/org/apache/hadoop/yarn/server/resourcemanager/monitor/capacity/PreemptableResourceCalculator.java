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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}
 */
public class PreemptableResourceCalculator
    extends
      AbstractPreemptableResourceCalculator {
  private static final Log LOG =
      LogFactory.getLog(PreemptableResourceCalculator.class);

  /**
   * PreemptableResourceCalculator constructor
   *
   * @param preemptionContext
   * @param isReservedPreemptionCandidatesSelector this will be set by
   * different implementation of candidate selectors, please refer to
   * TempQueuePerPartition#offer for details.
   * @param allowQueuesBalanceAfterAllQueuesSatisfied
   */
  public PreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    super(preemptionContext, isReservedPreemptionCandidatesSelector,
        allowQueuesBalanceAfterAllQueuesSatisfied);
  }

  /**
   * This method computes (for a single level in the tree, passed as a {@code
   * List<TempQueue>}) the ideal assignment of resources. This is done
   * recursively to allocate capacity fairly across all queues with pending
   * demands. It terminates when no resources are left to assign, or when all
   * demand is satisfied.
   *
   * @param rc resource calculator  资源计算器
   * @param queues a list of cloned queues to be assigned capacity to (this is
   * an out param) 同一个父节点的所有队列
   * @param totalPreemptionAllowed total amount of preemption we allow  这一轮允许被抢占的总资源
   * @param tot_guarant the amount of capacity assigned to this pool of queues  父队列的理想资源（资源最小配置）
   */
  protected void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueuePerPartition> queues, Resource totalPreemptionAllowed,
      Resource tot_guarant) {

    // qAlloc tracks currently active queues (will decrease progressively as
    // demand is met)
    List<TempQueuePerPartition> qAlloc = new ArrayList<>(queues);
    // unassigned tracks how much resources are still to assign, initialized
    // with the total capacity for this set of queues
    Resource unassigned = Resources.clone(tot_guarant);

    // group queues based on whether they have non-zero guaranteed capacity
    Set<TempQueuePerPartition> nonZeroGuarQueues = new HashSet<>();
    Set<TempQueuePerPartition> zeroGuarQueues = new HashSet<>();

    for (TempQueuePerPartition q : qAlloc) {
      // 遍历同一个父队列的所有子队列，有最小资源配置的存放在nonZeroGuarQueues，没有最小配置的存放在zeroGuarQueues
      if (Resources.greaterThan(rc, tot_guarant,
          q.getGuaranteed(), Resources.none())) {
        nonZeroGuarQueues.add(q);
      } else {
        zeroGuarQueues.add(q);
      }
    }

    // first compute the allocation as a fixpoint based on guaranteed capacity
    // 根据最小资源配置计算资源分布
    computeFixpointAllocation(tot_guarant, new HashSet<>(nonZeroGuarQueues),
        unassigned, false);

    // if any capacity is left unassigned, distributed among zero-guarantee
    // queues uniformly (i.e., not based on guaranteed capacity, as this is zero)
    if (!zeroGuarQueues.isEmpty()
        && Resources.greaterThan(rc, tot_guarant, unassigned, Resources.none())) {
      // 没有设置最小资源分配的话，则平均分配
      computeFixpointAllocation(tot_guarant, zeroGuarQueues, unassigned,
          true);
    }

    // based on ideal assignment computed above and current assignment we derive
    // how much preemption is required overall
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueuePerPartition t:queues) {
      //总的抢占资源 = 当前队列使用资源 - 队列理想分布资源
      if (Resources.greaterThan(rc, tot_guarant,
          t.getUsed(), t.idealAssigned)) {
        Resources.addTo(totPreemptionNeeded, Resources
            .subtract(t.getUsed(), t.idealAssigned));
      }
    }

    /**
     * if we need to preempt more than is allowed, compute a factor (0<f<1)
     * that is used to scale down how much we ask back from each queue
     */
    float scalingFactor = 1.0F;
    // 总的抢占资源 > 此轮允许抢占的资源，则进行缩放。
    if (Resources.greaterThan(rc,
        tot_guarant, totPreemptionNeeded, totalPreemptionAllowed)) {
      scalingFactor = Resources.divide(rc, tot_guarant, totalPreemptionAllowed,
          totPreemptionNeeded);
    }

    // assign to each queue the amount of actual preemption based on local
    // information of ideal preemption and scaling factor
    for (TempQueuePerPartition t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
  }

  /**
   * This method recursively computes the ideal assignment of resources to each
   * level of the hierarchy. This ensures that leafs that are over-capacity but
   * with parents within capacity will not be preemptionCandidates. Preemptions
   * are allowed within each subtree according to local over/under capacity.
   *
   * @param root the root of the cloned queue hierachy
   * @param totalPreemptionAllowed maximum amount of preemption allowed
   */
  protected void recursivelyComputeIdealAssignment(
      TempQueuePerPartition root, Resource totalPreemptionAllowed) {
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // compute ideal distribution at this level
      // 计算该层下各个子队列的理想分布
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);

      // compute recursively for lower levels and build list of leafs
      for (TempQueuePerPartition t : root.getChildren()) {
        recursivelyComputeIdealAssignment(t, totalPreemptionAllowed);
      }
    }
  }

  private void calculateResToObtainByPartitionForLeafQueues(
      Set<String> leafQueueNames, Resource clusterResource) {
    // Loop all leaf queues
    for (String queueName : leafQueueNames) {
      // 跳过没有开启抢占的队列
      if (context.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      // compute resToObtainByPartition considered inter-queue preemption
      for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
        // we act only if we are violating balance by more than
        // maxIgnoredOverCapacity
        // 如果队列当前使用资源量大于用户配置的最低资源保证*(1 + maxIgnoredOverCapacity)，则允许对此队列进行资源抢占。
        if (Resources.greaterThan(rc, clusterResource,
            qT.getUsed(), Resources
                .multiply(qT.getGuaranteed(),
                    1.0 + context.getMaxIgnoreOverCapacity()))) {
          /*
           * We introduce a dampening factor naturalTerminationFactor that
           * accounts for natural termination of containers.
           *
           * This is added to control pace of preemption, let's say:
           * If preemption policy calculated a queue *should be* preempted 20 GB
           * And the nature_termination_factor set to 0.1. As a result, preemption
           * policy will select 20 GB * 0.1 = 2GB containers to be preempted.
           *
           * However, it doesn't work for YARN-4390:
           * For example, if a queue needs to be preempted 20GB for *one single*
           * large container, preempt 10% of such resource isn't useful.
           * So to make it simple, only apply nature_termination_factor when
           * selector is not reservedPreemptionCandidatesSelector.
           */

          //实际抢占资源需乘以用户配置的naturalTerminationFactor参数。
          Resource resToObtain = qT.toBePreempted;
          if (!isReservedPreemptionCandidatesSelector) {
            if (Resources.greaterThan(rc, clusterResource, resToObtain,
                Resource.newInstance(0, 0))) {
              resToObtain = Resources.multiplyAndNormalizeUp(rc, qT.toBePreempted,
                  context.getNaturalTerminationFactor(), Resource.newInstance(1, 1));
            }
          }

          // Only add resToObtain when it >= 0
          if (Resources.greaterThan(rc, clusterResource, resToObtain,
              Resources.none())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Queue=" + queueName + " partition=" + qT.partition
                  + " resource-to-obtain=" + resToObtain);
            }
          }
          // 设置真正被抢占的资源
          qT.setActuallyToBePreempted(Resources.clone(resToObtain));
        } else {
          qT.setActuallyToBePreempted(Resources.none());
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(qT);
        }
      }
    }
  }

  private void updatePreemptableExtras(TempQueuePerPartition cur) {
    if (cur.children == null || cur.children.isEmpty()) {
      //更新该队列抢占附加信息
      cur.updatePreemptableExtras(rc);
    } else {
      //如果存在子队列，则更新子队列的抢占附加信息
      for (TempQueuePerPartition child : cur.children) {
        updatePreemptableExtras(child);
      }
      cur.updatePreemptableExtras(rc);
    }
  }

  public void computeIdealAllocation(Resource clusterResource,
      Resource totalPreemptionAllowed) {
    for (String partition : context.getAllPartitions()) {
      //获取root队列
      TempQueuePerPartition tRoot = context.getQueueByPartition(
          CapacitySchedulerConfiguration.ROOT, partition);
      //更新队列资源可被抢占的附加信息，基本为（当前使用资源 - 队列最小配置资源）
      updatePreemptableExtras(tRoot);

      // compute the ideal distribution of resources among queues
      // updates cloned queues state accordingly
      //初始化root的理想资源分配为配置的最小资源
      tRoot.initializeRootIdealWithGuarangeed();
      //计算队列间资源的理想分布(重新计算并设置各子队列理想最低资源)
      recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);
    }

    // based on ideal allocation select containers to be preempted from each
    // calculate resource-to-obtain by partition for each leaf queues
    // 基于理想资源分布队列，选在需要被抢占的container
    calculateResToObtainByPartitionForLeafQueues(context.getLeafQueueNames(),
        clusterResource);
  }
}