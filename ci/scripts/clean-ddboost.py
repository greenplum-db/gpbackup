#!/usr/bin/python3

from time import sleep
import os
import pexpect

targetIPAddrPass = {
    os.getenv("DD_SOURCE_HOST", ""): os.getenv("DD_SYSADMIN_PW", ""),
    os.getenv("DD_DEST_HOST", ""): os.getenv("DD_SYSADMIN_PW", ""),
}

for ipaddr in targetIPAddrPass:
    pw = targetIPAddrPass.get(ipaddr, "NONE")
    target = "ssh sysadmin@{}".format(ipaddr)

    command = "{} 'ddboost storage-unit delete GPDB'".format(target)
    print "%s" % command
    child = pexpect.spawn(command, timeout=1200)

    # in the case of first-time connection, handle key fingerprint prompt
    resp = child.expect(["continue connecting","Password:"])
    if resp == 0:
        print "Encountered fingerprint prompt"
        child.sendline("yes")
        sleep(2)
        child.expect("Password:")
        child.sendline(pw)

    else:
        print "Did not encounter fingerprint prompt"
        child.sendline(pw)
    print "%s" % child.before + child.after
    sleep(3)

    command = "{} 'ddboost storage-unit delete gpdb_boostfs'".format(target)
    print "%s" % command
    child = pexpect.spawn(command, timeout=1200)
    child.expect("Password:")
    child.sendline(pw)
    print "%s" % child.before + child.after
    sleep(5)  # give it a chance to process before cleaning

    command = "{} 'filesys clean start'".format(target)
    print "%s" % command
    child = pexpect.spawn(command, timeout=1200)
    child.expect("Password:")
    child.sendline(pw)
    print "%s" % child.before + child.after
    sleep(5)  # give it a chance to start cleaning

    command = "{} 'filesys clean watch'".format(target)
    print "%s" % command
    child = pexpect.spawn(command, timeout=1200)
    child.expect("Password:")
    child.sendline(pw)
    child.expect(pexpect.EOF)
    sleep(3)

    command = "{} 'ddboost storage-unit create GPDB user gpadmin'".format(target)
    print "%s" % command
    child = pexpect.spawn(command, timeout=1200)
    child.expect("Password:")
    child.sendline(pw)
    print "%s" % child.before + child.after
    sleep(3)

    command = "{} 'ddboost storage-unit create gpdb_boostfs user gpadmin'".format(
        target
    )
    print "%s" % command
    child = pexpect.spawn(command, timeout=1200)
    child.expect("Password:")
    child.sendline(pw)
    print "%s" % child.before + child.after
    sleep(3)

    print "Finished cleaning: %s" % ipaddr
