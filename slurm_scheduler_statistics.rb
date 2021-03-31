# frozen_string_literal: true

# Report the scheduler and backfill statistics from sdiag
class SlurmSchedulerStatistics
  def initialize(collector, config)
    @collector = collector
    @config = config
    @sdiag_categories = %w[General Main Backfilling RPC]
    
    # Regex to split sdiag output into General/Main scheduler/Backfilling/RPC
    @sdiag_split_regex = /^Main schedule statistics \(microseconds\):|
                          ^Backfilling stats|
                          ^Remote Procedure Call statistics by message type/x
    @sdiag_parse_regex = /^\s*([\w \t()]+):\s+(\d+)/
    
    @stats_to_report = {
      'General' => [],
      'Main' => [
        'Last cycle',         # time (us) for last scheduling cycle
        'Mean cycle',         # max time (us) for scheduling cycle since restart
        'Cycles per minute',
        'Last queue length'
      ],
      'Backfilling' => [
        'Total backfilled jobs (since last stats cycle start)',
        'Last cycle',
        'Mean cycle',
        'Last depth cycle',             # no. jobs considered for backfilling
        'Last depth cycle (try sched)', # startable jobs considered
        'Depth Mean',
        'Depth Mean (try depth)',
        'Queue length mean',
        'Last table size', # no. of time slots considered for backfilling
        'Mean table size',
        'Latency for 1000 calls to gettimeofday()'
      ],
      'RPC' => []
    }
  end

  def array_to_categorised_hash(categories, data)
    zipped_array = categories.zip(data)
    categorised_hash = zipped_array.to_h
  end

  def parse_hash(sdiag_hash, pattern)
    sdiag_hash.each do |category, raw_string|
      matches = raw_string.scan(pattern).to_h
      sdiag_hash[category] = matches
    end

    hash
  end

  def filter_stats(stats, filter_hash)
    stats.each do |category, category_stats|
      # iterate over categories
      filter = filter_hash[category]
      # keep only statistics listed in the filter
      category_stats.keep_if { |stat, _| filter.include? stat }
    end
  end

  def raid
    sdiag = `sdiag`
    
    sdiag_split = sdiag.split(@sdiag_split_regex)

    sdiag_hash = array_to_categorised_hash(@sdiag_categories, sdiag_split)
    
    stats = parse_hash(sdiag_hash)

    stats = filter_stats(stats, stats_to_report) 

    stats.each do |category, category_stats|
      next unless category_stats.empty?

      category_lower = category.downcase
      category_stats.each do |stat, val|
        help = "Slurm scheduler (#{category}): #{stat}"
        description = help.downcase.gsub(/\s+/, '_').gsub(/[():]/, '')
        
        @collector.report!(
          description,
          val,
          help: help,
          type: gauge
        )
      end
    end
  end
end
