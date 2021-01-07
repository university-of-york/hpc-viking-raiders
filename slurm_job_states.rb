class SlurmJobStates
  def initialize(collector, config)
    @collector = collector
    @interval = config[:raid_every]
  end

  def raid
    start_time = (Time.now - @interval).strftime("%H:%M:%S")

    # Get raw data from sacct and read jobs into an array
    raw = `/usr/bin/sacct -a -P -o State,Partition -S #{start_time}`.lines
    # remove any whitespace from the ends of each string
    raw = raw.map(&:strip)
    # drop the header line
    raw = raw[1..-1]
    # split each line into state and partition
    raw = raw.map{ |l| l.split("|") }
    # and remove the "by xxxxxx" from CANCELLED jobs
    raw = raw.map { |state, partition| [state.split[0], partition] }

    # Make a tally of each state/partition combo
    tally = raw.tally

    # Clean up any previously reported metrics
    # to prevent stale labelsets
    @collector.redact!("slurm_job_states")

    # Report new metrics
    tally.each do |labelset, number|
      @collector.report!(
        "slurm_job_states",
        number,
        help: "Number of jobs in each state",
        type: "gauge",
        labels: {state: labelset[0], partition: labelset[1]}
      )
    end
  end
end
