namespace :hot_topic do
  desc "count this week hot topics"
  task count_hot_weekly: :environment do
    Topic.calculate_hot_weekly
  end

  desc "count today's hot topics"
  task count_hot_daily: :environment do
    Topic.calculate_hot_daily
  end

end
