require 'date'

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
      '-a',                     # all jobs
      '-X',                     # allocations
      '-P',                     # "|" - delimited 
      '--partition=nodes',
      '--state=CD',
      '-o AllocCPUs,Submit,Start',  
      '-S #{start_time}',
      '-E #{end_time}'
    ].join(' ')
    
    # get raw data from sacct and read jobs into an array
    data = `#{squeue_cmd}`.lines
    # remove any whitespace from the ends of each string
    data = data.map(&:strip)
    # drop the header line
    data = data[1..-1]
    # split each line by observables
    data = data.map{ |row| row.split("|") }
    # calculate queue time from (start - submit) time
    data = data.map{ |job| job = [job[0], DateTime.parse(job[2]).to_time.to_i - DateTime.parse(job[1]).to_time.to_i] }
    
    # bin data by cores
    bin_cores = [1, 10, 30, 100, 250, 500]  # upper bounds
    




