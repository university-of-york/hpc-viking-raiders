# frozen_string_literal: true

# Report the number of jobs currently in queue,
# aggregated by state, user, and partition
class SlurmNumberJobs
  def initialize(collector, config)
    @collector = collector
    @config = config
  end

  def raid
    raw = `squeue --format="%P,%u,%T" --noheader`
    raw = raw.lines.map(&:strip)
    raw = raw.map { |line| line.split(',') }

    tally = raw.tally

    @collector.redact!('slurm_number_jobs')

    tally.each do |labelset, number|
      @collector.report!(
        'slurm_number_jobs',
        number,
        help: 'Number of jobs for a given user, partition, and state',
        type: 'gauge',
        labels: {
          partition: labelset[0],
          user: labelset[1],
          state: labelset[2]
        }
      )
    end
  end
end
