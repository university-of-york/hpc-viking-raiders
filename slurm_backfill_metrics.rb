# frozen_string_literal: true

# Pull backfill scheduler metrics from sdiag and report
class SlurmPendingOnQos
  def initialize(collector, config)
    @collector = collector
    @config = config
  end

  def raid
    data = `sdiag`.split("\n")
    backfill_offset = data.index(data.grep(/Backfilling stat.+/)[0])
    data = data[backfill_offset..backfill_offset + 16].map(&:split)
  end
end
