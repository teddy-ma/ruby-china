# coding: utf-8
require 'rails_helper'
require 'rake'

describe Topic, :type => :model do
  let(:topic) { FactoryGirl.create(:topic) }
  let(:user) { FactoryGirl.create(:user) }

  it "should no save invalid node_id" do
    expect(FactoryGirl.build(:topic, :node_id => 0).valid?).not_to be_truthy
  end

  it "should set last_active_mark on created" do
    # because the Topic index is sort by replied_at,
    # so the new Topic need to set a Time, that it will display in index page
    expect(Factory(:topic).last_active_mark).not_to be_nil
  end

  it "should not update last_active_mark on save" do
    last_active_mark_was = topic.last_active_mark
    topic.save
    expect(topic.last_active_mark).to eq(last_active_mark_was)
  end

  it "should get node name" do
    node = Factory :node
    expect(Factory(:topic, :node => node).node_name).to eq(node.name)
  end

  describe "#push_follower, #pull_follower" do
    let(:t) { FactoryGirl.create(:topic, :user_id => 0) }
    it "should push" do
      t.push_follower user.id
      expect(t.follower_ids.include?(user.id)).to be_truthy
    end

    it "should pull" do
      t.pull_follower user.id
      expect(t.follower_ids.include?(user.id)).not_to be_truthy
    end

    it "should not push when current_user is topic creater" do
      allow(t).to receive(:user_id).and_return(user.id)
      expect(t.push_follower(user.id)).to eq(false)
      expect(t.follower_ids.include?(user.id)).not_to be_truthy
    end
  end

  it "should update after reply" do
    reply = Factory :reply, :topic => topic, :user => user
    expect(topic.last_active_mark).not_to be_nil
    expect(topic.replied_at.to_i).to eq(reply.created_at.to_i)
    expect(topic.last_reply_id).to eq(reply.id)
    expect(topic.last_reply_user_id).to eq(reply.user_id)
    expect(topic.last_reply_user_login).to eq(reply.user.login)
  end

  it "should update after reply without last_active_mark when the topic is created at month ago" do
    allow(topic).to receive(:created_at).and_return(1.month.ago)
    allow(topic).to receive(:last_active_mark).and_return(1)
    reply = Factory :reply, :topic => topic, :user => user
    expect(topic.last_active_mark).not_to eq(reply.created_at.to_i)
    expect(topic.last_reply_user_id).to eq(reply.user_id)
    expect(topic.last_reply_user_login).to eq(reply.user.login)
  end

  it "should covert body with Markdown on create" do
    t = Factory(:topic, :body => "*foo*")
    expect(t.body_html).to eq("<p><em>foo</em></p>")
  end


  it "should covert body on save" do
    t = Factory(:topic, :body => "*foo*")
    old_html = t.body_html
    t.body = "*bar*"
    t.save
    expect(t.body_html).not_to eq(old_html)
  end

  it "should not store body_html when it not changed" do
    t = Factory(:topic, :body => "*foo*")
    t.body = "*fooaa*"
    allow(t).to receive(:body_changed?).and_return(false)
    old_html = t.body_html
    t.save
    expect(t.body_html).to eq(old_html)
  end

  it "should log deleted user name when use destroy_by" do
    t = Factory(:topic)
    t.destroy_by(user)
    expect(t.who_deleted).to eq(user.login)
    expect(t.deleted_at).not_to eq(nil)
    t1 = Factory(:topic)
    expect(t1.destroy_by(nil)).to eq(false)
  end

  describe "#auto_space_with_en_zh" do
    it "should auto fix on save" do
      topic.title = "Gitlab怎么集成GitlabCI"
      topic.save
      topic.reload
      expect(topic.title).to eq("Gitlab 怎么集成 GitlabCI")
    end
  end

  describe "#excellent" do
    it "should suggest a topic as excellent" do
      topic.excellent = 1
      topic.save
      expect(Topic.excellent).to include(topic)
    end
  end

  describe ".update_last_reply" do
    it "should work" do
      t = Factory(:topic)
      old_updated_at = t.updated_at
      r = Factory(:reply, topic: t)
      expect(t.update_last_reply(r)).to be_truthy
      expect(t.replied_at).to eq r.created_at
      expect(t.last_reply_id).to eq r.id
      expect(t.last_reply_user_id).to eq r.user_id
      expect(t.last_reply_user_login).to eq r.user.login
      expect(t.last_active_mark).not_to be_nil
      expect(t.updated_at).not_to eq old_updated_at
    end

    it "should update with nil when have :force" do
      t = Factory(:topic)
      r = Factory(:reply, topic: t)
      t.update_last_reply(nil, force: true)
      expect(t.replied_at).to be_nil
      expect(t.last_reply_id).to be_nil
      expect(t.last_reply_user_id).to be_nil
      expect(t.last_reply_user_login).to be_nil
      expect(t.last_active_mark).not_to be_nil
    end
  end

  describe ".update_deleted_last_reply" do
    let(:t) { Factory(:topic) }
    context "when have last Reply and param it that Reply" do
      it "last reply should going to previous Reply" do
        r0 = Factory(:reply, topic: t)
        r1 = Factory(:reply, topic: t)
        expect(t.last_reply_id).to eq r1.id
        expect(t).to receive(:update_last_reply).with(r0, force: true)
        t.update_deleted_last_reply(r1)
      end

      it "last reply will be nil" do
        r = Factory(:reply, topic: t)
        expect(t.update_deleted_last_reply(r)).to be_truthy
        t.reload
        expect(t.last_reply_id).to be_nil
        expect(t.last_reply_user_login).to be_nil
        expect(t.last_reply_user_id).to be_nil
      end
    end

    context "when param is nil" do
      it "should work" do
        expect(t.update_deleted_last_reply(nil)).to be_falsey
      end
    end

    context "when last reply is not equal param" do
      it "should do nothing" do
        r0 = Factory(:reply, topic: t)
        r1 = Factory(:reply, topic: t)
        expect(t.update_deleted_last_reply(r0)).to be_falsey
        expect(t.last_reply_id).to eq r1.id
      end
    end
  end

  describe "#log_viewed" do
    let(:t) { Factory(:topic) }
    before(:each) do
      $redis.flushall
    end
    context "when redis key is nil" do
      it "should work" do
        t.log_viewed
        weekly_view_hash = $redis.hgetall("topic_view:#{Time.now.strftime('%Y%m%d')}")
        daily_view_hash  = $redis.hgetall("topic_view:#{Time.now.strftime('%Y%m%d%H')}")
        expect(weekly_view_hash["topic:#{t.id}"]).to eq("1")
        expect(daily_view_hash["topic:#{t.id}"]).to eq("1")
      end
    end

    context "when topic viewed again" do
      it "should plus one to the value" do
        t.log_viewed
        t.log_viewed
        weekly_view_hash = $redis.hgetall("topic_view:#{Time.now.strftime('%Y%m%d')}")
        daily_view_hash  = $redis.hgetall("topic_view:#{Time.now.strftime('%Y%m%d%H')}")
        expect(weekly_view_hash["topic:#{t.id}"]).to eq("2")
        expect(daily_view_hash["topic:#{t.id}"]).to eq("2")
      end
    end
  end

  describe "#log_replyed" do
    let(:t) { Factory(:topic) }
    # let(:r) { Factory :reply, :topic => t }
    before(:each) do
      $redis.flushall
    end
    context "when the topic is first time replyed" do
      it "should work" do
        t.log_replyed
        hash = $redis.hgetall("topic_reply:#{Time.now.strftime('%Y%m%d')}")
        expect(hash["topic:#{t.id}"]).to eq("1")
      end
    end
  end

  describe "#log_reply_deleted" do
    let(:t) { Factory(:topic) }
    # let(:r) { Factory :reply, :topic => t }
    before(:each) do
      $redis.flushall
    end

    context "when the redis key exists" do
      it "should minus one" do

        available_time = Time.now - 1.day
        $redis.hset("topic_reply:#{available_time.strftime('%Y%m%d')}", "topic:#{t.id}", "12")
        t.log_reply_deleted(available_time)
        hash = $redis.hgetall("topic_reply:#{available_time.strftime('%Y%m%d')}")
        expect(hash["topic:#{t.id}"]).to eq("11")
      end
    end

    context "when the redis key not exists" do
      it "should do nothing" do
         old_time = Time.now - 1.year
         t.log_reply_deleted(old_time)
         expect({}). to eq($redis.hgetall("topic_reply:#{old_time.strftime('%Y%m%d')}"))
      end
    end

  end

  describe "calculate hot score" do
    let(:t1) { FactoryGirl.create(:topic, :created_at => Time.now) }
    let(:t2) { FactoryGirl.create(:topic, :created_at => Time.now - 1.day) }
    let(:t3) { FactoryGirl.create(:topic, :created_at => Time.now - 2.day) }
    let(:t4) { FactoryGirl.create(:topic, :created_at => Time.now - 3.day) }
    let(:t5) { FactoryGirl.create(:topic, :created_at => Time.now - 4.day) }
    let(:t6) { FactoryGirl.create(:topic, :created_at => Time.now - 5.day) }
    let(:t7) { FactoryGirl.create(:topic, :created_at => Time.now - 6.day) }

    before(:each) do
      $redis.flushall
      load File.expand_path("../../../lib/tasks/hot_topic.rake", __FILE__)
      Rake::Task.define_task(:environment)
    end
    context "calculate weekly hot" do
      it "should work" do
        (1..7).each do |i|
          i.times do
            eval("t#{i}.log_replyed")
          end
        end
        hash = $redis.hgetall("topic_reply:#{Time.now.strftime('%Y%m%d')}")
        expect(hash["topic:#{t1.id}"]).to eq("1")
        expect(hash["topic:#{t7.id}"]).to eq("7")
        Rake::Task['hot_topic:count_hot_weekly'].invoke
        expect([t7.id,t6.id,t5.id,t4.id,t3.id,t2.id,t1.id].map(&:to_s))
          .to eq($redis.zrevrange("current_hot_weekly", 0, 6))
      end
    end
  end

end
