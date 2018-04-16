import pandas as pd
import re
from datetime import datetime

# df = pd.DataFrame(columns=['tag_no','tag_foll_no','DateTime','tag_name','logLine_text'])
# pattern = re.compile("<(\d+)>(\d+)\s(.*?)\sr001.pvt.bridges.psc.edu\s(.*?)\s[\s-]+(.*)")
# with open("03:22:2016 Logfile.txt") as file:
#     for line in file:
#         res = re.match(pattern,line)
#         df.loc[len(df)] = [res.group(1),res.group(2),res.group(3),res.group(4),res.group(5)]
# df.to_csv("03-22-Log file.csv")

df = pd.read_csv("03-22-Log file.csv")
df['tag_name_foll_no'] = df['tag_name'].apply(lambda r: 0 if len(r.split(' '))==1 else r.split(' ')[1])
df['tag_name'] = df['tag_name'].apply(lambda r: r if len(r.split(' '))==0 else r.split(' ')[0])
df['comments'] = ""
# print(df.loc[:,['tag_name','tag_name_foll_no']])
# print(df['tag_name'].value_counts())

df.loc[df['tag_name']=="dracut",'comments'] = "Reading Files Access and Installing modules"
df.loc[df['tag_name']=="systemd",'comments'] = "Failed to read PID; SMTP Correspondence; Stopping irqbalance daemon...; Config Files; Reloading"
df.loc[df['tag_name']=="sshd",'comments'] = "Identifaction failed; Accepted publickey; Session Failure- No such file or directory; Received Disconnect; Session Closed"
df.loc[df['tag_name']=="systemd-logind",'comments'] = "New Session; Removed Session"
df.loc[df['tag_name']=="puppet-agent",'comments'] = "enable changed 'false' to 'true'; Applied catalog;mode changed '0755' to '1777'; Triggered 'refresh'; defined content"
df.loc[df['tag_name']=="CROND",'comments'] = "sbin/naemon_update.sh; lib64/sa/sa1 1 1;run-parts /etc/cron.hourly';/usr/lib64/sa/sa2 -A;! /bin/systemctl -q is-active fetch-crl-cron.service || /usr/sbin/fetch-crl -q -r 360"
df.loc[df['tag_name']=="kernel",'comments'] = "unknown partition table; SGI XFS with ACLs, security attributes, no debug enabled;Mounting V4 Filesystem; Ending clean mount;eno1: link is not ready;igb 0000:02:00.0 eno1: igb: eno1 NIC Link is Up 1000 Mbps Full Duplex, Flow Control: RX; eno1: link becomes ready; enabling connected mode will cause multicast packet drops;mtu > 2044 will cause multicast packet drops.;fuse init (API version 7.22); can't use GFP_NOIO for QPs on device hfi1_0, using GFP_KERNEL"
df.loc[df['tag_name']=="nslcd",'comments'] = "<passwd=\"monitor\"> (re)loading /etc/nsswitch.conf;  Can't contact LDAP server"
df.loc[df['tag_name']=="run-parts(",'comments'] = "starting 0anacron;finished 0anacron;starting 0yum-hourly.cron;finished 0yum-hourly.cron;starting 0yum-daily.cron;finished 0yum-daily.cron;starting logrotate;finished logrotate;starting man-db.cron;finished man-db.cron;starting mlocate;finished mlocate"
df.loc[df['tag_name']=="nscd",'comments'] = "`/etc/netgroup': No such file or directory;Monitoring File; Monitored File; stat failed for file `/etc/netgroup'; will try again later: No such file or directory"
df.loc[df['tag_name']=="crontab",'comments'] = "(root) LIST (root)"
df.loc[df['tag_name']=="avahi-daemon",'comments'] = "Withdrawing address record;Leaving mDNS multicast group;no longer relevant for mDNS.;Registering new address record;Withdrawing workstation service;Host name conflict;Registering new address record"
df.loc[df['tag_name']=="ntpd",'comments'] = "Deleting interface;new interface(s) found: waking up resolver;0.0.0.0 c628 08 no_sys_peer;Listen normally on 8 "
df.loc[df['tag_name']=="anacron",'comments'] = "Anacron started on 2016-03-22;Will run job `cron.daily' in 21 min.;Will run job `cron.weekly' in 41 min.;Will run job `cron.monthly' in 61 min.;Jobs will be executed sequentially;Job `cron.daily' started;Job `cron.daily' terminated;Job `cron.weekly' started;Job `cron.weekly' terminated;Job `cron.monthly' started;Job `cron.monthly' terminated;Normal exit (3 jobs run)"
df.loc[df['tag_name']=="dhclient",'comments'] = "DHCPRequest; DHCPPACK;bound to 10.4.216.2"
df.loc[df['tag_name']=="sendmail",'comments'] = "who sent mail to who"
df.loc[df['tag_name']=="network",'comments'] = "Shutting down interface eno1:  [  OK  ];Shutting down interface ib0:  [  OK  ];Shutting down loopback interface:  [  OK  ];Bringing up loopback interface:  [  OK  ];Bringing up interface eno1:;Determining IP information for eno1... done.;[  OK  ];Bringing up interface ib0:  [  OK  ]"
df.loc[df['tag_name']=="yum",'comments'] = "Updated and Installed"
df.loc[df['tag_name']=="rhnsd",'comments'] = "/etc/sysconfig/rhn/systemid does not exist or is unreadable"
df.loc[df['tag_name']=="rsyslogd-2307",'comments'] = "warning: ~ action is deprecated"
df.loc[df['tag_name']=="rdma-ndd",'comments'] = "Node Descriptor format (%h %d);hfi1_0: change"
df.loc[df['tag_name']=="sm-msp-queue",'comments'] = "starting daemon (8.14.7): queueing@01:00:00;Mail send"
df.loc[df['tag_name']=="usermod",'comments'] = "who sent mail to who"
df.loc[df['tag_name']=="NET",'comments'] = "/usr/sbin/dhclient-script : updated /etc/resolv.conf"
df.loc[df['tag_name']=="rsyslogd",'comments'] = "origin software http://www.rsyslog.com start"
#
# df.to_csv("03-22-Log file.csv")

df = pd.read_csv("03-22-Log file.csv")
df.drop(labels=['Unnamed: 0', 'Unnamed: 0.1'],axis=1,inplace=True)
df['Date'] = df['DateTime'].apply(lambda r: re.search(r'(.*)T(.*)-',r).group(1))
df['Time'] = df['DateTime'].apply(lambda r: re.search(r'(.*)T(.*)-',r).group(2))
df['Hour'] = df['Time'].apply(lambda r: r.split(":")[0])
# print(df['Hour'].value_counts())
# print(df.loc[:,['tag_name','tag_no']].value_counts())
print(df.groupby(['Hour',"tag_name"]).size().to_string())