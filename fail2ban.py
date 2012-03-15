#!/usr/bin/python
#
# Fail2Ban is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Fail2Ban is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Fail2Ban; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#
#
# Fail2ban jail state fetcher for collectd
# Author: Antti Jaakkola
# Email:  annttu@annttu.fi
# based on fail2ban-client code
#

import sys, string, os, logging
import socket

# Inserts our own modules path first in the list
# fix for bug #343821
sys.path.insert(1, "/usr/share/fail2ban")

# Now we can import our modules
from client.csocket import CSocket
from client.configurator import Configurator

# Gets the instance of the logger.
logSys = logging.getLogger("fail2ban.client")

##
#
# @todo This class needs cleanup.

class Fail2banClient:
    def __init__(self):
        self.__stream = None
        self.__configurator = Configurator()
        self.__conf = dict()
        self.__conf["conf"] = "/etc/fail2ban"
        self.__conf["socket"] = None
        logSys.setLevel(logging.WARNING)
        stdout = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(levelname)-6s %(message)s')
        stdout.setFormatter(formatter)
        logSys.addHandler(stdout)

        self.__configurator.setBaseDir(self.__conf["conf"])
        self.__configurator.readEarly()
        socket = self.__configurator.getEarlyOptions()
        if self.__conf["socket"] == None:
            self.__conf["socket"] = socket["socket"]
        logSys.info("Using socket file " + self.__conf["socket"])

    def __processCmd(self, jail = "", listjails = False, showRet = True):
        cmd = []
        if listjails is True:
            cmd.append(['status'])
        elif jail != "":
            cmd.append(['status', jail])
        for c in cmd:
            retval = False
            try:
                logSys.debug("%s" % c)
                client = CSocket(self.__conf["socket"])
                ret = client.send(c)
                if ret[0] == 0:
                    if listjails is False:
                        retval =  ret[1][1][1][0][1]
                    else:
                        retval = [i.strip() for i in ret[1][1][1].split(",")]
                    if showRet:
                        logSys.debug("OK : " + `ret[1]`)
                        logSys.debug("retval: %s" % retval)
                    return retval
                else:
                    logSys.debug("NOK: " + `ret[1].args`)
                    logSys.debug(ret[1])
                    return
            except socket.error:
                if showRet:
                    logSys.error("Unable to contact server. Is it running?")
                return
            except Exception, e:
                if showRet:
                    logSys.error(e)
                return

    def get_banned(self, jail):
        return self.__processCmd(jail)

    def list_jails(self):
        return self.__processCmd(listjails = True)

class ServerExecutionException(Exception):
    pass

client = None
def init():
    client = Fail2banClient()
    return True

def read(data=None):
    global client
    jails = client.list_jails()
    for jail in jails:
        v1 = collectd.Values(type='gauge', interval=10)
        v1.plugin='fail2ban-jails-' + jail
        v1.dispatch(values=client.get_banned(jail))

collectd.register_read(read)
collectd.register_init(init)
