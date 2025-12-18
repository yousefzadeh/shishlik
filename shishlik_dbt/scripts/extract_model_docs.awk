#! /usr/bin/awk -f
/DOC START/,/DOC END/{if($0 !~ /DOC START|DOC END/) print $0}
