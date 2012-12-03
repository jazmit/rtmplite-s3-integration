'''
Test the load on the server by doing lot of connections.
'''

import sys, rtmpclient, multitask

_usage = '''usage: python test_load.py publishers players
  publishers: number of publishers, one per connection.
  players: number of players per publisher, one per connection.
'''

def test1(publishers, players):
    duration = 30
    for i in xrange(publishers):
        url, stream = 'rtmp://localhost/live%d'%(i,), 'live'
        multitask.add(rtmpclient.connect(url, publishFile='file1.flv', publishStream=stream, playStream=None, duration=duration, params=[]))
        for j in xrange(players):
            multitask.add(rtmpclient.connect(url, playStream=stream, duration=duration, params=[]))

# The main routine to invoke the copy method
if __name__ == '__main__':
    if len(sys.argv) < 3: print _usage; sys.exit(-1)
    _debug = sys.argv[1] == '-d'
    
    try:
        test1(int(sys.argv[-2]), int(sys.argv[-1]))
        multitask.run()
    except KeyboardInterrupt:
        if _debug: print 'keyboard interrupt'
