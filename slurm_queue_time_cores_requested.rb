require 'date'

# extend Array to support mean, median
class Array
  def mean(array)
    return nil if array.empty?
    array = array.sum.fdiv(array.size)
  end

  def median(array)
    return nil if array.empty?
    array = array.sort
    mid = array.size / 2  # midpoint
    # if length is odd, select middle element; otherwise take mean of two middle elements
    return array.size % 2 == 1 ? array[mid] : mean(array[mid-1..mid])
  end
end

class SlurmQueueTimesCoresRequested
  def initialize(collector, config)
    @collector = collector
    @interval = config[:raid_every]
  end

  def raid
    start_time = (Time.now - @interval).strftime("%H:%M:%S")
    end_time = Time.now.strftime("%H:%M:%S")

    sacct_cmd = [
      'sacct',
      '-a', # all jobs
      '-X', # allocations
      '-P', # "|" - delimited 
      '--partition=nodes',
      '--state=CD,CF,CG,DL,F,NF,OOM,PR,R,ST,TO',  # consider all job states that have had resources allocated since the
      '-o AllocCPUs,Submit,Start',                # last cycle
      '-S #{start_time}', 
      '-E #{end_time}'
    ].join(' ')
    
    # get raw data from sacct and read jobs into an array
    data = `#{squeue_cmd}`.lines
    # remove any whitespace from the ends of each string
    data.map!(&:strip)
    # drop the header line
    data = data[1..-1]
    # number of jobs considered
    njobs = data.length()

    # don't bother calculating if this is 0
    if njobs > 0
      # split each line by observables
      data.map! { |row| row.split("|") }
      
      # calculate queue time from (start - submit) time
      data.map! do |job|
        job = [job[0].to_i,
               (DateTime.parse(job[2]).to_time - DateTime.parse(job[1]).to_time).to_i]
      end
      
      # create bin intervals (AllocCPUs)
      bins_cores = [1, 10, 30, 100, 250, 500]                  # upper bin boundaries
                     .prepend(-1)                              # prepend -1 to ensure first bin starts at 0
                     .each_cons(2)                             # consider consecutive pairs
                     .map { |lower, upper| [lower+1, upper] }  # generate bin lower/upper limit pairs
      
      # bin queue time by core
      binned_by_core = bins_cores.map do |bin|
        data
          .select{ |cores, time| cores.between?(bin[0], bin[1]) } # find matching jobs for bin
          .map{ |job| job[1] }                                    # queue time
      end
      
      qt_stats = ['mean', 'median', 'max']
      qt_by_core = binned_by_core.map do |bin|
        mean_qt = bin.mean
        median_qt = bin.median
        max_qt = bin.max
        bin = [mean_qt, median_qt, max_qt]
      end

      # report mean, median, max queue time for each CPU core bin
      qt_by_core.each.with_index do |bin, bin_idx|    # iterate over bins
        bin.each.with_index do |queuetime, stat_idx|  # iterate over mean, median, max
          @collector.report!(
            "queue_time_cores_requested",
            queuetime,
            help: "Queue time binned by number of CPU cores requested",
            type: "gauge",
            labels: {cores_min: bins_cores[bin_idx][0],
                     cores_max: bins_cores[bin_idx][1],
                     statistic: qt_stats[stat_idx]}
          )
        end
      end

    else
      # no jobs
    end
  end
end

   






