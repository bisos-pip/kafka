#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" #+begin_org
* ~[Summary]~ :: A =CmndSvc= for admin-facing information about this host's
  =kafka.here= Kafka (KRaft, single node) + Kafbat UI installation: listeners,
  service status, paths, topics, consumer groups and config drift.
#+end_org """

####+BEGIN: b:py3:cs:file/dblockControls :classification "cs-mu"
""" #+begin_org
* [[elisp:(org-cycle)][| /Control Parameters Of This File/ |]] :: dblk ctrls classifications=cs-mu
#+BEGIN_SRC emacs-lisp
(setq-local b:dblockControls t) ; (setq-local b:dblockControls nil)
(put 'b:dblockControls 'py3:cs:Classification "cs-mu") ; one of cs-mu, cs-u, cs-lib, bpf-lib, pyLibPure
#+END_SRC
#+RESULTS:
: cs-mu
#+end_org """
####+END:

####+BEGIN: b:prog:file/proclamations :outLevel 1
""" #+begin_org
* *[[elisp:(org-cycle)][| Proclamations |]]* :: Libre-Halaal Software --- Part Of BISOS ---  Poly-COMEEGA Format.
** This is Libre-Halaal Software. © Neda Communications, Inc. Subject to AGPL.
** It is part of BISOS (ByStar Internet Services OS)
** Best read and edited  with Blee in Poly-COMEEGA (Polymode Colaborative Org-Mode Enhance Emacs Generalized Authorship)
#+end_org """
####+END:

####+BEGIN: b:prog:file/particulars :authors ("./inserts/authors-mb.org")
""" #+begin_org
* *[[elisp:(org-cycle)][| Particulars |]]* :: Authors, version
** This File: /bxRepos/bisos-pip/airflow/py3/bin/airflowAdmin.cs
** Authors: Mohsen BANAN, http://mohsen.banan.1.byname.net/contact
#+end_org """
####+END:

####+BEGIN: b:py3:file/particulars-csInfo :status "inUse"
""" #+begin_org
* *[[elisp:(org-cycle)][| Particulars-csInfo |]]*
#+end_org """
import typing
csInfo: typing.Dict[str, typing.Any] = { 'moduleName': ['airflowAdmin'], }
csInfo['version'] = '202409222627'
csInfo['status']  = 'inUse'
csInfo['panel'] = 'airflowAdmin-Panel.org'
csInfo['groupingType'] = 'IcmGroupingType-pkged'
csInfo['cmndParts'] = 'IcmCmndParts[common] IcmCmndParts[param]'
####+END:

""" #+begin_org
* [[elisp:(org-cycle)][| ~Description~ |]] :: [[file:/bisos/git/auth/bxRepos/blee-binders/bisos-core/PyFwrk/bisos-pip/bisos.cs/_nodeBase_/fullUsagePanel-en.org][BISOS CmndSvcs Panel]]   [[elisp:(org-cycle)][| ]]

This is an admin-facing =CmndSvc= for this host's =kafka.here= installation ---
Apache Kafka as a single-node KRaft broker+controller, and Kafbat UI as its web UI:
the listeners and where things live, links for operating the =kafka-sysd.pcs= and
=kafbat-ui-sysd.pcs= units, topic and consumer-group CLI invocations, and a
read-only =driftCheck= of the live config files and units against
=bisos.kafka.kafkaInfo=. Modelled on =bisos.airflow='s =airflowAdmin.cs=.

** Status: Starting point -- not yet complete
** /[[elisp:(org-cycle)][| Planned Improvements |]]/ :
*** TODO Live status Cmnds (broker reachable, quorum leader, topic count) rather than printed examples
#+end_org """

####+BEGIN: b:prog:file/orgTopControls :outLevel 1
""" #+begin_org
* [[elisp:(org-cycle)][| Controls |]] :: [[elisp:(delete-other-windows)][(1)]] | [[elisp:(show-all)][Show-All]]  [[elisp:(org-shifttab)][Overview]]  [[elisp:(progn (org-shifttab) (org-content))][Content]] | [[file:Panel.org][Panel]] | [[elisp:(blee:ppmm:org-mode-toggle)][Nat]] | [[elisp:(bx:org:run-me)][Run]] | [[elisp:(bx:org:run-me-eml)][RunEml]] | [[elisp:(progn (save-buffer) (kill-buffer))][S&Q]]  [[elisp:(save-buffer)][Save]]  [[elisp:(kill-buffer)][Quit]] [[elisp:(org-cycle)][| ]]
** /Version Control/ ::  [[elisp:(call-interactively (quote cvs-update))][cvs-update]]  [[elisp:(vc-update)][vc-update]] | [[elisp:(bx:org:agenda:this-file-otherWin)][Agenda-List]]  [[elisp:(bx:org:todo:this-file-otherWin)][ToDo-List]]

#+end_org """
####+END:

####+BEGIN: b:py3:file/workbench :outLevel 1
""" #+begin_org
* [[elisp:(org-cycle)][| Workbench |]] :: [[elisp:(python-check (format "/bisos/venv/py3/bisos3/bin/python -m pyclbr %s" (bx:buf-fname))))][pyclbr]] || [[elisp:(python-check (format "/bisos/venv/py3/bisos3/bin/python -m pydoc ./%s" (bx:buf-fname))))][pydoc]] || [[elisp:(python-check (format "/bisos/pipx/bin/pyflakes %s" (bx:buf-fname)))][pyflakes]] | [[elisp:(python-check (format "/bisos/pipx/bin/pychecker %s" (bx:buf-fname))))][pychecker (executes)]] | [[elisp:(python-check (format "/bisos/pipx/bin/pycodestyle %s" (bx:buf-fname))))][pycodestyle]] | [[elisp:(python-check (format "/bisos/pipx/bin/flake8 %s" (bx:buf-fname))))][flake8]] | [[elisp:(python-check (format "/bisos/pipx/bin/pylint %s" (bx:buf-fname))))][pylint]]  [[elisp:(org-cycle)][| ]]
#+end_org """
####+END:

####+BEGIN: b:py3:cs:framework/imports :basedOn "classification"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CsFrmWrk   [[elisp:(outline-show-subtree+toggle)][||]] *Imports* =Based on Classification=cs-mu=
#+end_org """
from bisos import b
from bisos.b import cs
from bisos.b import b_io
from bisos.common import csParam

import collections
####+END:

# The facts of the installation --- account, versions, paths, listeners, fqdn, and the
# CLI form for by-hand invocations --- come from bisos.kafka.kafkaInfo, the one
# source the sbom, the units, dns and vhost read too.
import pathlib

from bisos.basics import pyRunAs
from bisos.kafka.kafkaInfo import (
    kafkaInfo, serverPropertiesContent, envFileContent, kafbatEnvFileContent,
    kvDrift, unitDrift,
)

""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CsFrmWrk   [[elisp:(outline-show-subtree+toggle)][||]] ~csuList emacs-list Specifications~  [[elisp:(blee:org:code-block/above-run)][ /Eval Below/ ]] [[elisp:(org-cycle)][| ]]
#+BEGIN_SRC emacs-lisp
(setq  b:py:cs:csuList
  (list
   ;; "bisos.b.cs.ro"
   ;; "bisos.csPlayer.bleep"
   ;; "bisos.facter.facter_csu"
   ;; "bisos.banna.bannaPortNu"
 ))
#+END_SRC
#+RESULTS:
#+end_org """

####+BEGIN: b:py3:cs:framework/csuListProc :pyImports t :csuImports t :csuParams t :csmuParams nil
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CsFrmWrk   [[elisp:(outline-show-subtree+toggle)][||]] ~Process CSU List~ with /0/ in csuList pyImports=t csuImports=t csuParams=t
#+end_org """


csuList = [ ]

g_importedCmndsModules = cs.csuList_importedModules(csuList)

def g_extraParams():
    csParams = cs.param.CmndParamDict()
    cs.csuList_commonParamsSpecify(csuList, csParams)
    cs.argsparseBasedOnCsParams(csParams)

####+END:

####+BEGIN: b:py3:cs:main/exposedSymbols :classes ()
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CsFrmWrk   [[elisp:(outline-show-subtree+toggle)][||]] ~CS Controls and Exposed Symbols List Specification~ with /0/ in Classes List
#+end_org """
####+END:

cs.invOutcomeReportControl(cmnd=True, ro=True)

####+BEGIN: blee:bxPanel:foldingSection :outLevel 0 :sep nil :title "CmndSvcs" :anchor ""  :extraInfo "Command Services Section"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*     [[elisp:(outline-show-subtree+toggle)][| _CmndSvcs_: |]]  Command Services Section  [[elisp:(org-shifttab)][<)]] E|
#+end_org """
####+END:

####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "examples" :extent "verify" :ro "noCli" :comment "FrameWrk: CS-Main-Examples" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<examples>>  *FrameWrk: CS-Main-Examples*  =verify= ro=noCli   [[elisp:(org-cycle)][| ]]
#+end_org """
class examples(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}
    rtInvConstraints = cs.rtInvoker.RtInvoker.new_noRo() # NO RO From CLI

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:
        """FrameWrk: CS-Main-Examples"""
        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        self.cmndDocStr(f""" #+begin_org
***** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Conventional top level example.
        #+end_org """)

        cs.examples.myName(cs.G.icmMyName(), cs.G.icmMyFullName())
        cs.examples.commonBrief()

        cmnd = cs.examples.cmndEnter

        cs.examples.menuChapter('=Kafka.Here -- Host, Listeners, Paths and Logs=')
        cmnd('hostInfo', comment=" # fqdn, the three banna ports, install/config/data/log paths")

        cs.examples.menuChapter('=Kafka.Here -- systemd Units=')
        cmnd('servicesInfo', comment=" # links to kafka-sysd.pcs and kafbat-ui-sysd.pcs")

        cs.examples.menuChapter('=Kafka.Here -- Topics and Messages=')
        cmnd('topicsInfo', comment=" # create, list, describe, produce, consume")

        cs.examples.menuChapter('=Kafka.Here -- Consumer Groups=')
        cmnd('consumerGroupsInfo', comment=" # list, describe (lag), reset offsets")

        cs.examples.menuChapter('=Kafka.Here -- Do Installation And Code Agree?=')
        cmnd('driftCheck', comment=" # read-only: live config files and units against bisos.kafka.kafkaInfo")

        cs.examples.menuChapter('=Full Overview=')
        cmnd('fullUpdate', comment=" # Show hostInfo + servicesInfo + topicsInfo + consumerGroupsInfo")

        return(cmndOutcome)

####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "fullUpdate" :comment "" :extent "verify" :ro "cli" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<fullUpdate>>  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class fullUpdate(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:

        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print hostInfo, servicesInfo, topicsInfo and consumerGroupsInfo together.
        #+end_org """): return(cmndOutcome)

        self.captureRunStr(""" #+begin_org
#+begin_src sh :results output :session shared
  kafkaAdmin.cs -i fullUpdate
#+end_src
#+RESULTS:
#+begin_example
#+end_example
        #+end_org """)

        hostInfo().pyCmnd()
        servicesInfo().pyCmnd()
        topicsInfo().pyCmnd()
        consumerGroupsInfo().pyCmnd()

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "hostInfo" :comment "" :extent "verify" :ro "cli" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<hostInfo>>  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class hostInfo(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:

        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print this host's Kafka listeners, Kafbat UI address and where things live.
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert

        cs.examples.menuSection('/Host/')
        literal(f"echo {kafkaInfo.fqdn}   # fqdn -- see py3/bin/kafka-here-dns.pcs")
        literal(f"{kafkaInfo.baseUrl}   # Kafbat UI -- open in a browser")
        literal(f"curl -sI {kafkaInfo.kafbatLocalUrl}   # Kafbat UI directly, bypassing nginx")

        cs.examples.menuSection('/Listeners -- ports from bisos.banna tcpPorts/')
        literal(f"echo {kafkaInfo.bootstrapServers}   # broker, PLAINTEXT -- what clients use as bootstrap.servers")
        literal(f"echo {kafkaInfo.controllerAddr}   # KRaft controller")
        literal(f"ss -ltnp | grep -E ':({kafkaInfo.portNu}|{kafkaInfo.controllerPortNu}|{kafkaInfo.kafbatPortNu}) '")

        cs.examples.menuSection('/Installation -- versions are pinned in bisos.kafka.kafkaInfo/')
        literal(f"ls -l {kafkaInfo.homeLink}   # -> {kafkaInfo.distDir}")
        literal(f"{kafkaInfo.binPath('kafka-topics')} --version")
        literal(f"ls -l {kafkaInfo.kafbatJar}")

        cs.examples.menuSection('/Config -- root:kafka 640, written only when absent/')
        literal(f"sudo cat {kafkaInfo.serverProps}")
        literal(f"sudo cat {kafkaInfo.envFile}")
        literal(f"sudo cat {kafkaInfo.kafbatEnvFile}")

        cs.examples.menuSection('/Data -- KRaft metadata and partition logs/')
        literal(f"sudo cat {kafkaInfo.metaProps}   # cluster.id, node.id")
        literal(f"sudo du -sh {kafkaInfo.dataDir}")
        literal(f"sudo -u {kafkaInfo.acctName} {kafkaInfo.binPath('kafka-metadata-quorum')} --bootstrap-controller {kafkaInfo.controllerAddr} describe --status")

        cs.examples.menuSection('/Logs/')
        literal(f"ls -la {kafkaInfo.logDir}")
        literal(f"tail -f {kafkaInfo.logDir}/server.log")
        literal("journalctl -u kafka.service -f")
        literal("journalctl -u kafbat-ui.service -f")

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "servicesInfo" :comment "" :extent "verify" :ro "cli" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<servicesInfo>>  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class servicesInfo(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:

        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print links for operating the kafka and kafbat-ui units
        (broker first, then the UI).
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert

        cs.examples.menuSection('/Kafka broker+controller/')
        literal("py3/bin/kafka-sysd.pcs -i examples   # list this unit's Cmnds")
        literal("systemctl status kafka.service")
        literal("systemctl restart kafka.service")

        cs.examples.menuSection('/Kafbat UI/')
        literal("py3/bin/kafbat-ui-sysd.pcs -i examples")
        literal("systemctl status kafbat-ui.service")
        literal("systemctl restart kafbat-ui.service")

        cs.examples.menuSection('/kafka.here -- dns and nginx vhost/')
        literal("py3/bin/kafka-here-dns.pcs -i dnsCap_verify")
        literal("py3/bin/kafka-wvd.pcs -i webVd_verify")
        literal("py3/bin/kafka-assemble.cs -i fullUpdate   # both of the above, updated")

        cs.examples.menuSection('/CBS/CBM assembly (sbom + 2 units + assemble as one bundle)/')
        literal("py3/bin/kafka-cbs.pcs -i examples")
        literal("py3/bin/cbmProc-kafka.spcs -i examples")

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "topicsInfo" :extent "verify" :comment "Topics -- create, list, describe, produce, consume" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<topicsInfo>>  *Topics -- create, list, describe, produce, consume*  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class topicsInfo(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:
        """Topics -- create, list, describe, produce, consume"""
        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print the topic-level CLI invocations, --bootstrap-server already supplied.
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert
        _topic = "bisosTest"

        cs.examples.menuSection('/Topics/')
        literal(f"{kafkaInfo.cli('kafka-topics')} --list")
        literal(f"{kafkaInfo.cli('kafka-topics')} --create --topic {_topic} --partitions 1 --replication-factor 1")
        literal(f"{kafkaInfo.cli('kafka-topics')} --describe --topic {_topic}")
        literal(f"{kafkaInfo.cli('kafka-topics')} --delete --topic {_topic}")

        cs.examples.menuSection('/Produce and Consume/')
        literal(f"echo hello | {kafkaInfo.cli('kafka-console-producer')} --topic {_topic}")
        literal(f"{kafkaInfo.cli('kafka-console-consumer')} --topic {_topic} --from-beginning --max-messages 10")

        cs.examples.menuSection('/Topic Config/')
        literal(f"{kafkaInfo.cli('kafka-configs')} --entity-type topics --entity-name {_topic} --describe")
        literal(f"{kafkaInfo.cli('kafka-configs')} --entity-type topics --entity-name {_topic} --alter --add-config retention.ms=86400000")

        cs.examples.menuSection('/Broker/')
        literal(f"{kafkaInfo.cli('kafka-broker-api-versions')} | head -1   # reachable?")
        literal(f"{kafkaInfo.cli('kafka-log-dirs')} --describe")

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "consumerGroupsInfo" :extent "verify" :comment "Consumer groups -- list, lag, reset" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<consumerGroupsInfo>>  *Consumer groups -- list, lag, reset*  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class consumerGroupsInfo(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:
        """Consumer groups -- list, lag, reset"""
        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print the consumer-group CLI invocations. Kafbat UI shows the same, live.
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert
        _group = "<group>"

        cs.examples.menuSection('/Consumer Groups/')
        literal(f"{kafkaInfo.cli('kafka-consumer-groups')} --list")
        literal(f"{kafkaInfo.cli('kafka-consumer-groups')} --describe --group {_group}   # LAG per partition")
        literal(f"{kafkaInfo.cli('kafka-consumer-groups')} --describe --all-groups --state")

        cs.examples.menuSection('/Reset Offsets -- group must have no active members/')
        literal(f"{kafkaInfo.cli('kafka-consumer-groups')} --group {_group} --reset-offsets --to-earliest --all-topics --dry-run")
        literal(f"{kafkaInfo.cli('kafka-consumer-groups')} --group {_group} --reset-offsets --to-earliest --all-topics --execute")

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "driftCheck" :comment "" :extent "verify" :ro "cli" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<driftCheck>>  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class driftCheck(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:

        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Compare the live installation with bisos.kafka.kafkaInfo. Read-only.
        #+end_org """): return(cmndOutcome)

        # The three files are root:kafka 640, so they are read as root. Nothing is written.
        # They are written only when absent: hand overrides are legitimate, and generator lines
        # added later never reach existing hosts. "missing" is therefore a finding, not an error.
        for livePath, generated in (
                (kafkaInfo.serverProps, serverPropertiesContent()),
                (kafkaInfo.envFile, envFileContent()),
                (kafkaInfo.kafbatEnvFile, kafbatEnvFileContent()),
        ):
            print(f"* {livePath}")
            if not livePath.exists():   # stat needs no root; reading does
                print("  not installed")
                continue
            liveText = pyRunAs.as_root_readFromFile(livePath)
            if not liveText:
                print(f"  cannot read {livePath}")
                continue
            drift = kvDrift(liveText, generated)
            for key in drift["missing"]:
                print(f"  missing  {key}   (generator has it, live file does not)")
            for key, expected, live in drift["differs"]:
                print(f"  differs  {key}   expected={expected}   live={live}")
            for key in drift["extra"]:
                print(f"  extra    {key}   (not generated -- a hand addition)")
            if not (drift["missing"] or drift["differs"] or drift["extra"]):
                print("  agrees with the generator")

        for unitName, envFile in (("kafka", kafkaInfo.envFile), ("kafbat-ui", kafkaInfo.kafbatEnvFile)):
            unitPath = pathlib.Path(f"/etc/systemd/system/{unitName}.service")
            print(f"* {unitPath}")
            if not unitPath.exists():
                print("  not installed")
                continue
            lacking = unitDrift(unitPath.read_text(), envFile)
            if lacking:
                for line in lacking:
                    print(f"  missing  {line}")
            else:
                print("  identity lines agree (User, Group, EnvironmentFile)")

        print(f"* {kafkaInfo.homeLink}")
        if not kafkaInfo.homeLink.exists():
            print("  not installed")
        elif kafkaInfo.homeLink.resolve() != kafkaInfo.distDir:
            print(f"  points at {kafkaInfo.homeLink.resolve()}, kafkaInfo pins {kafkaInfo.distDir}")
        else:
            print(f"  points at the pinned {kafkaInfo.distName}")

        return cmndOutcome


####+BEGIN: blee:bxPanel:foldingSection :outLevel 0 :sep nil :title "Main" :anchor ""  :extraInfo "Framework DBlock"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*     [[elisp:(outline-show-subtree+toggle)][| _Main_: |]]  Framework DBlock  [[elisp:(org-shifttab)][<)]] E|
#+end_org """
####+END:

####+BEGIN: b:py3:cs:framework/main :csInfo "csInfo" :noCmndEntry "examples" :extraParamsHook "g_extraParams" :importedCmndsModules "g_importedCmndsModules"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CsFrmWrk   [[elisp:(outline-show-subtree+toggle)][||]] =g_csMain= (csInfo, _examples_, g_extraParams, g_importedCmndsModules)
#+end_org """

if __name__ == '__main__':
    cs.main.g_csMain(
        csInfo=csInfo,
        noCmndEntry=examples,  # specify a Cmnd name
        extraParamsHook=g_extraParams,
        importedCmndsModules=g_importedCmndsModules,
    )

####+END:

####+BEGIN: b:py3:cs:framework/endOfFile :basedOn "classification"
""" #+begin_org
* [[elisp:(org-cycle)][| *End-Of-Editable-Text* |]] :: emacs and org variables and control parameters
#+end_org """

#+STARTUP: showall

### local variables:
### no-byte-compile: t
### end:
####+END:
