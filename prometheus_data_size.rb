class PrometheusDataSize
  def initialize(collector, config)
    @collector = collector
    @path = "/mnt/lustre/groups/prometheus/data"
  end

  def raid
    bytes = `du -sb #{@path}`.split[0].to_i

    @collector.redact!("prometheus_data_size")
    @collector.report!(
      "prometheus_data_size",
      bytes,
      help: "Size of the prometheus data directory in bytes",
      type: "gauge",
      labels: { path: @path }
    )
  end
end
