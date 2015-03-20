# coding: utf-8
require "auto-space"
class Topic
  include Mongoid::Document
  include Mongoid::Timestamps
  include Mongoid::BaseModel
  include Mongoid::SoftDelete
  include Mongoid::CounterCache
  include Mongoid::Likeable
  include Mongoid::MarkdownBody
  include Redis::Objects
  include Mongoid::Mentionable

  field :title
  field :body
  field :body_html
  field :last_reply_id, type: Integer
  field :replied_at , type: DateTime
  field :source
  field :message_id
  field :replies_count, type: Integer, default: 0
  # 回复过的人的 ids 列表
  field :follower_ids, type: Array, default: []
  field :suggested_at, type: DateTime
  # 最后回复人的用户名 - cache 字段用于减少列表也的查询
  field :last_reply_user_login
  # 节点名称 - cache 字段用于减少列表也的查询
  field :node_name
  # 删除人
  field :who_deleted
  # 用于排序的标记
  field :last_active_mark, type: Integer
  # 是否锁定节点
  field :lock_node, type: Mongoid::Boolean, default: false
  # 精华帖 0 否， 1 是
  field :excellent, type: Integer, default: 0

  # 临时存储检测用户是否读过的结果
  attr_accessor :read_state

  belongs_to :user, inverse_of: :topics
  counter_cache name: :user, inverse_of: :topics
  belongs_to :node
  counter_cache name: :node, inverse_of: :topics
  belongs_to :last_reply_user, class_name: 'User'
  belongs_to :last_reply, class_name: 'Reply'
  has_many :replies, dependent: :destroy

  validates_presence_of :user_id, :title, :body, :node

  index node_id: 1
  index user_id: 1
  index last_active_mark: -1
  index likes_count: 1
  index suggested_at: 1
  index excellent: -1

  counter :hits, default: 0

  delegate :login, to: :user, prefix: true, allow_nil: true
  delegate :body, to: :last_reply, prefix: true, allow_nil: true

  # scopes
  scope :last_actived, -> {  desc(:last_active_mark) }
  # 推荐的话题
  scope :suggest, -> { where(:suggested_at.ne => nil).desc(:suggested_at) }
  scope :fields_for_list, -> { without(:body,:body_html) }
  scope :high_likes, -> { desc(:likes_count, :_id) }
  scope :high_replies, -> { desc(:replies_count, :_id) }
  scope :no_reply, -> { where(replies_count: 0) }
  scope :popular, -> { where(:likes_count.gt => 5) }
  scope :without_node_ids, Proc.new { |ids| where(:node_id.nin => ids) }
  scope :excellent, -> { where(:excellent.gte => 1) }

  def self.find_by_message_id(message_id)
    where(message_id: message_id).first
  end

  # 排除隐藏的节点
  def self.without_hide_nodes
    where(:node_id.nin => self.topic_index_hide_node_ids)
  end

  def self.topic_index_hide_node_ids
    SiteConfig.node_ids_hide_in_topics_index.to_s.split(",").collect { |id| id.to_i }
  end

  before_save :store_cache_fields
  def store_cache_fields
    self.node_name = self.node.try(:name) || ""
  end
  before_save :auto_space_with_title
  def auto_space_with_title
    self.title.auto_space!
  end

  before_create :init_last_active_mark_on_create
  def init_last_active_mark_on_create
    self.last_active_mark = Time.now.to_i
  end

  def push_follower(uid)
    return false if uid == self.user_id
    return false if self.follower_ids.include?(uid)
    self.push(follower_ids: uid)
    true
  end

  def pull_follower(uid)
    return false if uid == self.user_id
    self.pull(follower_ids: uid)
    true
  end

  def update_last_reply(reply, opts = {})
    # replied_at 用于最新回复的排序，如果帖着创建时间在一个月以前，就不再往前面顶了
    return false if reply.blank? && !opts[:force]

    self.last_active_mark = Time.now.to_i if self.created_at > 1.month.ago
    self.replied_at = reply.try(:created_at)
    self.last_reply_id = reply.try(:id)
    self.last_reply_user_id = reply.try(:user_id)
    self.last_reply_user_login = reply.try(:user_login)
    self.save
  end

  # 更新最后更新人，当最后个回帖删除的时候
  def update_deleted_last_reply(deleted_reply)
    return false if deleted_reply.blank?
    return false if self.last_reply_user_id != deleted_reply.user_id

    previous_reply = self.replies.where(:_id.nin => [deleted_reply.id]).recent.first
    self.update_last_reply(previous_reply, force: true)
  end

  # 删除并记录删除人
  def destroy_by(user)
    return false if user.blank?
    self.update_attribute(:who_deleted,user.login)
    self.destroy
  end

  def destroy
    super
    delete_notifiaction_mentions
  end

  def last_page_with_per_page(per_page)
    page = (self.replies_count.to_f / per_page).ceil
    page > 1 ? page : nil
  end

  # 所有的回复编号
  def reply_ids
    Rails.cache.fetch([self,"reply_ids"]) do
      self.replies.only(:_id).map(&:_id)
    end
  end

  def excellent?
    self.excellent >= 1
  end

  def log_viewed
    $redis.hincrby "topic_view:#{Time.now.strftime('%Y%m%d')}",   "topic:#{self.id}", 1
    $redis.hincrby "topic_view:#{Time.now.strftime('%Y%m%d%H')}", "topic:#{self.id}", 1
  end

  def log_replyed
    $redis.hincrby "topic_reply:#{Time.now.strftime('%Y%m%d')}",  "topic:#{self.id}", 1
    $redis.hincrby "topic_view:#{Time.now.strftime('%Y%m%d%H')}", "topic:#{self.id}", 1
  end

  # 删除回复的话需要删除回帖创建时间的 key
  def log_reply_deleted(replyed_at)
    $redis.hincrby "topic_reply:#{replyed_at.strftime('%Y%m%d')}",   "topic:#{self.id}", -1
    $redis.hincrby "topic_reply:#{replyed_at.strftime('%Y%m%d%H')}", "topic:#{self.id}", -1
  end

  # def self.calculate_hot_weekly
  #   today = Time.now.to_date
  #   a_week_ago = today - 6
  #   hash = Hash.new
  #   (a_week_ago..today).each_with_index do |date, i|
  #     calculate_by_datetime_from_redis(hash, date, i, '%Y%m%d')
  #   end
  #   # 这个 hash 按照 value 的值排序， 或者直接扔 redis 里让 redis 帮忙排序
  #   hash.map { |k,v| $redis.zadd "current_hot_weekly", v, k.split(":").last }
  #   # 取的时候直接 zrange current_hot_weekly 0 99
  # end
  #
  # def self.calculate_hot_daily
  #   hash = Hash.new
  #   (DateTime.now - 1.day).step((DateTime.now), 1.0/24).each_with_index do |date_with_hour, i|
  #     calculate_by_datetime_from_redis(hash, date_with_hour, i, '%Y%m%d%H')
  #   end
  #   hash.map { |k,v| $redis.zadd "current_hot_daily", v, k.split(":").last }
  # end
  #
  # def self.say_hello
  #   logger.info "hello log"
  #   puts "hello puts"
  # end
  #
  # private
  #
  # def self.calculate_by_datetime_from_redis(ret, date, coefficient, time_format)
  #   reply_hash = $redis.hgetall "topic_reply:#{date.strftime(time_format)}"
  #   reply_hash = reply_hash.map { |k, v| [k, v.to_i * ( coefficient + 1 ) * 3] }.to_h
  #   ret.merge!(reply_hash){|key, first, second| first + second }
  #   view_hash = $redis.hgetall "topic_view:#{date.strftime(time_format)}"
  #   view_hash = view_hash.map { |k, v| [k, v.to_i * ( coefficient + 1 )] }.to_h
  #   ret.merge!(view_hash){|key, first, second| first + second }
  # end
end
