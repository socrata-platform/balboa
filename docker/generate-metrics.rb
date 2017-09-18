#!/usr/bin/env ruby
require 'stomp'
require 'json'

amq_port = `docker inspect --format '{{ (index (index .NetworkSettings.Ports "61613/tcp") 0).HostPort }}' docker_activemq_1`.chomp.to_i
amq_host = {host: '127.0.0.1', port: amq_port}
p amq_host
client = Stomp::Client.open({
    :hosts => [amq_host],
    :max_reconnect_attempts => 3,
    :initial_reconnect_delay => 1,
    :parse_timeout => 10,
    :stompconn => true,
    :connect_headers => {
        :'accept-version' => '1.1',
        :host => 'localhost'
    }
})
current_dir = File.dirname(__FILE__)
json_file = File.expand_path('./large-metric-batch.json', current_dir)
message = JSON.load(File.open(json_file, 'r'))

client.publish('/queue/balboa-service-jms-queue', message.to_json, :suppress_content_length => true)