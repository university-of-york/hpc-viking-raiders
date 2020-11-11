# frozen_string_literal: true

require 'date'

# For each Viking partition, report the number of jobs pending for a long time
# due to QoS reasons.
class SlurmPendingOnQos
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
      squeue_cmd = [
        'squeue',
        '--format="%R,%V"',
        '--noheader',
        "--partition=#{partition}",
        '--state=PENDING'
      ].join(' ')

      data = `#{squeue_cmd}`.split("\n").grep(/QOS/).map do |row|
        row.split(',')
      end

      count = data.count do |columns|
        (Time.now.to_i - DateTime.parse(columns[1]).to_time.to_i) > threshold
      end

      @collector.report!(
        'pending_on_qos',
        count,
        help: 'Number of jobs pending beyond a threshold for QoS reasons',
        type: 'gauge',
        labels: {
          partition: partition.to_s
        }
      )
    end
  end
end
