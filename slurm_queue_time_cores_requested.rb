# frozen_string_literal: true

require 'date'

# extend Array to support mean, median
class Array
  def mean
    return nil if empty?

    sum.fdiv(size)
  end

  def median
    return nil if empty?

    sort
    mid = size / 2 # midpoint
    # if length is odd, select middle element; otherwise take mean of two
    # middle elements
    if size.odd?
      self[mid]
    else
      self[(mid - 1)..mid].sum.fdiv(2)
    end
  end
end

# For various job sizes (by cores requested), report the average and max queue
# times.
class SlurmQueueTimeCoresRequested
  def initialize(collector, config)
    @collector = collector
    @interval = config[:raid_every]
    @cpu_bin_upperbounds = [1, 10, 30, 100, 250, 500]
    @queue_time_stats = %w[mean median max]
    @gauge_name = 'slurm_queue_time_cores_requested'
  end

  def raid
    # get slurm data
    data = parse_sacct_data
    # don't report if no jobs match conditions
    return unless data.length.positive?

    # determine queue time
    data = calculate_queue_time(data)
    # create bins
    bins_cores = create_bins
    # bin data
    binned_by_core, qt_by_core = bin_data(bins_cores, data)
    # report binned data
    report_binned_data(bins_cores, binned_by_core, qt_by_core)
  end

  private

  def raw_sacct_data
    start_time = (Time.now - @interval).strftime('%H:%M:%S')
    end_time = Time.now.strftime('%H:%M:%S')

    sacct_cmd = [
      "sacct",
      "--allusers",
      "--allocations",
      "--parsable2", # "|" - delimited
      "--noheader",
      "--partition=nodes",
      "--state=CD", # consider only completed jobs
      "--format=AllocCPUs,Submit,Start",
      "--starttime=#{start_time}",
      "--endtime=#{end_time}"
    ].join(' ')

    # get raw data from sacct
    `#{sacct_cmd}`
  end

  def parse_sacct_data
    # read raw slurm data into array
    data = raw_sacct_data.lines

    # remove any whitespace from the ends of each string
    data.map!(&:strip)
    # split each line about delimiter
    data.map { |row| row.split('|') }
  end

  def calculate_queue_time(data)
    # calculate queue time from (start - submit) time
    data.map do |job|
      [job[0].to_i,
       (DateTime.parse(job[2]).to_time - DateTime.parse(job[1]).to_time).to_i]
    end
  end

  def create_bins
    # create bin intervals (AllocCPUs)
    @cpu_bin_upperbounds.prepend(-1)               # first bin starts at 0
                        .each_cons(2)              # consecutive values
                        .map { |l, u| [l + 1, u] } # lower/upper bin limits
  end

  def bin_data(bins_cores, data)
    # bin queue time by core
    binned_by_core = bins_cores.map do |bin|
      data
        .select { |cores, _time| cores.between?(bin[0], bin[1]) }
        .map { |job| job[1] } # extract queue time
    end

    # calculate stats for each bin
    qt_by_core = binned_by_core.map do |bin|
      @queue_time_stats.map { |stat| bin.send(stat.to_sym) }
    end

    [binned_by_core, qt_by_core]
  end

  def redact_data(*labels)
    if labels
      @collector.redact!(@gauge_name, labels: labels)
    else
      @collector.redact!(@gauge_name)
    end
  end

  def report_data(queue_time, labels)
    @collector.report!(
      @gauge_name,
      queue_time,
      help: 'Queue time binned by number of CPU cores requested',
      type: 'gauge',
      labels: labels
    )
  end

  def report_binned_data(bins_cores, binned_by_core, qt_by_core)
    # report mean, median, max queue time for each CPU core bin
    qt_by_core.each.with_index do |bin, bin_idx|    # iterate over bins
      bin.each.with_index do |queue_time, stat_idx| # iterate over statistics
        # redact empty bins
        if queue_time.nil?
          redact_data({ cores_min: bins_cores[bin_idx][0],
                        cores_max: bins_cores[bin_idx][1],
                        statistic: @queue_time_stats[stat_idx] })
        else
          report_data(queue_time,
                      { njobs: binned_by_core[bin_idx].length,
                        cores_min: bins_cores[bin_idx][0],
                        cores_max: bins_cores[bin_idx][1],
                        statistic: @queue_time_stats[stat_idx] })
        end
      end
    end
  end
end
