#
# Tests out usage of the general HBase query interface
#

from jyunit.util import *

import java.lang

from com.meetup.base.db.hbase import EntityMetadata, Query, Criteria, HBaseException
from com.meetup.feeds.db import ChapterFeedItem, DiscussionItem, FeedItem, FeedItemService


def query_by_member():
	memId=4679998
	srv = FeedItemService(FeedItem)
	q = srv.query()
	q.add( Criteria.eq( "memberId", java.lang.Integer(memId) ) )
	items = q.execute()

	cnt = 0
	assertMoreThan(len(items), 0, "member ID should have returned at least 1 item")
	for i in items:
		print "#%d: %s" % (cnt, i.id)
		cnt += 1
		assertEquals(i.getMemberId(), memId)
		print ""

def query_by_chapter():
	chapId=332719
	srv = FeedItemService(ChapterFeedItem)
	q = srv.query()
	q.add( Criteria.eq( "targetChapterId", java.lang.Integer(chapId) ) )
	items = q.execute()

	cnt = 0
	assertMoreThan(len(items), 0, "chapter ID should have returned at least 1 item")
	for i in items:
		print "#%d: %s" % (cnt, i.id)
		cnt += 1
		assertEquals(i.getTargetChapterId(), chapId)
		print ""



def query_by_chapter_type():
	srv = FeedItemService(ChapterFeedItem)
	q = srv.query()
	q.add( Criteria.eq( "targetChapterId", java.lang.Integer(1370934) ) ).add( Criteria.eq( "itemType", java.lang.String("new_discussion") ) )
	items = q.execute()

	cnt = 0
	assertMoreThan(len(items), 0, "chapter ID should have returned at least 1 item")
	for i in items:
		print "#%d: %s" % (cnt, i.id)
		cnt += 1
		assertEquals(i.getTargetChapterId(), 1370934)
		assertEquals(i.getItemType(), 'new_discussion')


def query_by_discussion():
	srv = FeedItemService(DiscussionItem)
	q = srv.query()
	q.add( Criteria.eq( "threadId", java.lang.Integer(6273891) ) )
	items = q.execute()

	cnt = 0
	assertMoreThan(len(items), 0, "thread ID should have returned at least 1 item")
	for i in items:
		print "#%d: %s" % (cnt, i.id)
		cnt += 1
		assertEquals(i.getThreadId(), 6273891)
		print ""

def run_test():
	query_by_member()
	query_by_chapter()
	query_by_chapter_type()
	query_by_discussion()


if __name__ == '__main__':
	run_test()
	



