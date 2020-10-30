# frozen_string_literal: true

# For each Viking partition, report the number of jobs pending due to QoS
# reasons.
class PendingOnQos
  def initialize(collector, config)
    @collector = collector
    @partition_thresholds = {
      nodes: 604_800,
      week: 1_209_600,
      month: 2_419_200,
      himem: 604_800,
      himem_week: 1_209_600,
      gpu: 604_800,
      interactive: 900,
      test: 900,
      preempt: 2_419_200
    }
  end

  def raid
    @partition_thresholds.each do |partition, threshold|
      start_time = (Time.now - @partition_thresholds[:partition])
                   .strftime('%Y-%m-:%d')

      squeue_cmd = [
        'squeue',
        '--format="%A,%R,%V"',
        '--noheader',
        '--parsable2',
        "--partition=#{partition}",
        '--state=PENDING'
      ].join(' ')
    end

    @collector.report!(
      name: 'pending_on_qos',
      value: 255,
      help: 'Number of jobs pending for QoS reasons',
      type: 'gauge',
      labels: { partition: 'nodes' }
    )
  end
end
