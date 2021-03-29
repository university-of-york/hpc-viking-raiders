# frozen_string_literal: true

# Report the scheduler and backfill statistics from sdiag
class SlurmSchedulerStatistics
  def initialize(collector, config)
    @collector = collector
    @sdiag_categories = ["General", "Main scheduler", "Backfilling"]
    @sdiag_split_rgx = /Main schedule statistics \(microseconds\):|Backfilling stats/
    @sdiag_rgx = /^\s*([\w \t()]+):\s+(\d+)/
    @stats_filter = [
      # General:
      [
        'Jobs pending',
        'Jobs running',
      ],
      # Main scheduler:
      [
        'Last cycle',         # time (us) for last scheduling cycle
        'Mean cycle',         # max time (us) for scheduling cycle since restart
        'Cycles per minute',
        'Last queue length',
      ],
      # Backfilling:
      [
        'Total backfilled jobs (since last stats cycle start)', # since 00:00 UTC
        'Last cycle',
        'Mean cycle',
        'Last depth cycle',             # no. jobs considered for backfilling
        'Last depth cycle (try sched)', # startable jobs considered 
        'Depth Mean',
        'Depth Mean (try depth)',
        'Queue length mean',
        'Last table size',  # no. time slots considered for backfilling
        'Mean table size',
        'Latency for 1000 calls to gettimeofday()',
      ],
    ]
  end

  def scan_array(array, pattern)
    matches = []

    array.each_with_index do |str,idx|
      match = str.scan(pattern)
      matches[idx] = match.to_h
    end
    
    return matches
  end

  def raid
    sdiag = `sdiag`
    # Split into general/main scheduler/backfilling stats
    sdiag_categorised = sdiag.split(@sdiag_split_rgx)

    stats_by_category = scan_array(sdiag_categorised, @sdiag_rgx)
  
    stats_by_category.each_with_index do |data,cat|
      @stats_filter[cat].each do |stat|
        val = data[stat]
        @collector.report!(
          'schduler_statistics',
          val,
          help: 'Slurm scheduler statistics',
          type: 'gauge',
          labels: {
            'category': @sdiag_categories[cat],
            'statistic': stat,
            'value': val,
          }
        )
      end
    end
  end
end
