namespace :hot_topic do
  desc "count this week hot topics"
  task count_hot_weekly: :environment do
    calculate_hot_weekly
  end

  desc "count today's hot topics"
  task count_hot_daily: :environment do
    calculate_hot_daily
  end

end


def calculate_hot_weekly
  today = Time.now.to_date
  a_week_ago = today - 6
  hash = Hash.new
  (a_week_ago..today).each_with_index do |date, i|
    calculate_by_datetime_from_redis(hash, date, i, '%Y%m%d')
  end
  $redis.del("current_hot_weekly")
  hash.map { |k,v| $redis.zadd "current_hot_weekly", v, k.split(":").last }
end

def calculate_hot_daily
  hash = Hash.new
  (DateTime.now - 1.day).step((DateTime.now), 1.0/24).each_with_index do |date_with_hour, i|
    calculate_by_datetime_from_redis(hash, date_with_hour, i, '%Y%m%d%H')
  end
  $redis.del("current_hot_daily")
  hash.map { |k,v| $redis.zadd "current_hot_daily", v, k.split(":").last }
end

private

def calculate_by_datetime_from_redis(ret, date, coefficient, time_format)
  reply_hash = $redis.hgetall "topic_reply:#{date.strftime(time_format)}"
  reply_hash = reply_hash.map { |k, v| [k, v.to_i * ( coefficient + 1 ) * 3] }.to_h
  ret.merge!(reply_hash){|key, first, second| first + second }
  view_hash = $redis.hgetall "topic_view:#{date.strftime(time_format)}"
  view_hash = view_hash.map { |k, v| [k, v.to_i * ( coefficient + 1 )] }.to_h
  ret.merge!(view_hash){|key, first, second| first + second }
end
