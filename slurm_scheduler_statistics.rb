# frozen_string_literal: true

# Report the scheduler and backfill statistics from sdiag
class SlurmSchedulerStatistics
  def initialize(collector, config)
    @collector = collector
    @sdiag_categories = ["General", "Main", "Backfilling"]
    @sdiag_split_rgx = /Main schedule statistics \(microseconds\):|Backfilling stats/
    @sdiag_rgx = /^\s*([\w \t()]+):\s+(\d+)/
    @stats_filter = [
      # General:
      [],
      # Main:
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
    # Split into general/main/backfilling stats
    sdiag_categorised = sdiag.split(@sdiag_split_rgx)

    stats_by_category = scan_array(sdiag_categorised, @sdiag_rgx)
  
    stats_by_category.each_with_index do |data,cat|
      if !@stats_filter[cat].empty?
        @stats_filter[cat].each do |stat|
          val = data[stat]
          category = @sdiag_categories[cat].downcase
          description = "Slurm scheduler (#{category}): #{stat}"
          
          @collector.report!(
            description.downcase.gsub(/\s+/, '_').gsub(/[():]/, ''),
            val,
            help: description,
            type: 'gauge',
          )
        end
      end
    end
  end
end
