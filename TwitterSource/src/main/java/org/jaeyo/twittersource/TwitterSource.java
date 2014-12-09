package org.jaeyo.twittersource;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import twitter4j.DirectMessage;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.auth.AccessToken;

import com.google.common.collect.ImmutableMap;

public class TwitterSource extends AbstractSource implements Configurable, UserStreamListener{
	private TwitterStream twitterStream;
	private ChannelProcessor channelProcessor;
	private ImmutableMap<String, String> params;
	
	@Override
	public void configure(Context context) {
		channelProcessor=getChannelProcessor();
		twitterStream=new TwitterStreamFactory().getInstance();
		
		String consumerKey = context.getString("consumerKey");
		String consumerSecret = context.getString("consumerSecret");
		String accessToken = context.getString("accessToken");
		String accessTokenSecret = context.getString("accessTokenSecret");
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
		twitterStream.addListener(this);
	
		params=context.getParameters();
	} // configure

	@Override
	public synchronized void start() {
		String type=params.get("type").toLowerCase();
		switch(type){
		case "keyword":
			String[] keywords=params.get("keywords").split(",");
			twitterStream.filter(new FilterQuery().track(keywords));
			break;
		case "usertimeline":
			twitterStream.user();
			break;
		default:
			throw new IllegalArgumentException(type);
		} //switch
	} //start

	@Override
	public synchronized void stop() {
		super.stop();
		twitterStream.shutdown();
	}

	@Override
	public void onException(Exception ex) {
		ex.printStackTrace();
	} 

	@Override
	public void onStatus(Status status) {
		Event event=EventBuilder.withBody(SerializationUtils.serialize(status));
		channelProcessor.processEvent(event);
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	}

	@Override
	public void onScrubGeo(long userId, long upToStatusId) {
	}

	@Override
	public void onStallWarning(StallWarning warning) {
	}

	@Override
	public void onDeletionNotice(long directMessageId, long userId) {
	}

	@Override
	public void onFriendList(long[] friendIds) {
	}

	@Override
	public void onFavorite(User source, User target, Status favoritedStatus) {
	}

	@Override
	public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
	}

	@Override
	public void onFollow(User source, User followedUser) {
	}

	@Override
	public void onUnfollow(User source, User unfollowedUser) {
	}

	@Override
	public void onDirectMessage(DirectMessage directMessage) {
	}

	@Override
	public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {
	}

	@Override
	public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {
	}

	@Override
	public void onUserListSubscription(User subscriber, User listOwner, UserList list) {
	}

	@Override
	public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {
	}

	@Override
	public void onUserListCreation(User listOwner, UserList list) {
	}

	@Override
	public void onUserListUpdate(User listOwner, UserList list) {
	}

	@Override
	public void onUserListDeletion(User listOwner, UserList list) {
	}

	@Override
	public void onUserProfileUpdate(User updatedUser) {
	}

	@Override
	public void onBlock(User source, User blockedUser) {
	}

	@Override
	public void onUnblock(User source, User unblockedUser) {
	}
} // class