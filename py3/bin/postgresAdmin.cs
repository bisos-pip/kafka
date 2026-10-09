#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" #+begin_org
* ~[Summary]~ :: A =CmndSvc= for admin-facing information about this host's
  =airflow.here= standalone×systemd Airflow installation: service status,
  log/dag locations, and users.
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

This is an admin-facing =CmndSvc= for this host's =airflow.here= standalone×systemd
Airflow installation: where the DAGs and logs live, links for operating the
=airflow-*-sysd.pcs= units, and where to find/manage users (with the initial
default admin password, if one was set at first =airflow users create=). It is a
starting-point converted from =bisos.dockerProc='s django-derived
=siteRegistrars-assemble.cs=; it will be expanded as bisos.airflow develops.

** Status: Starting point -- not yet complete
** /[[elisp:(org-cycle)][| Planned Improvements |]]/ :
*** TODO Read AIRFLOW_HOME / port / fqdn from bisos.airflow's own config rather than hardcoding
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

import pathlib

from bisos.banna import tcpPorts as bannaTcpPorts

_airflowFqdn = "airflow.here"
# AIRFLOW_HOME of the systemd-managed installation, matching what
# airflow-sbom.pcs writes to /etc/default/airflow and what the airflow-*.service
# units read via EnvironmentFile. Deliberately NOT pathlib.Path.home()/"airflow":
# the services run as User=airflow, so resolving the invoker's home would report
# whoever happens to run this CS rather than the installation being administered.
_airflowHome = pathlib.Path("/opt/airflow")
_airflowDagsDir = _airflowHome / "dags"
_airflowLogsDir = _airflowHome / "logs"
_airflowUser = "airflow"
_airflowBin = "/bisos/venv/py3/asc/bin/airflow"

# Prefix for every by-hand CLI invocation. Both halves matter:
#   sudo -u airflow  -- AIRFLOW_HOME and the SQLite metadata DB are airflow-owned;
#                       running the CLI as root leaves root-owned -wal/-shm files
#                       the services then cannot write.
#   env AIRFLOW_HOME -- /etc/default/airflow is an EnvironmentFile read by systemd,
#                       NOT by an interactive shell. Without this the CLI silently
#                       operates on ~airflow/airflow and reports an empty install.
_airflowCli = f"sudo -u {_airflowUser} env AIRFLOW_HOME={_airflowHome} {_airflowBin}"
_airflowPortNu = bannaTcpPorts.tcpPortsAssignedList.tcpPortsList['airflow'].portNu

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

        cs.examples.menuChapter('=Airflow.Here -- Host, DAGs and Logs=')
        cmnd('hostInfo', comment=" # fqdn, port, AIRFLOW_HOME, dags/logs dirs")

        cs.examples.menuChapter('=Airflow.Here -- systemd Units=')
        cmnd('servicesInfo', comment=" # links to the 4 airflow-*-sysd.pcs units")

        cs.examples.menuChapter('=Airflow.Here -- DAGs=')
        cmnd('dagsInfo', comment=" # list, trigger, pause/unpause, graph, parsing health")

        cs.examples.menuChapter('=Airflow.Here -- Users=')
        cmnd('usersInfo', comment=" # SimpleAuthManager: where users and passwords live")

        cs.examples.menuChapter('=Full Overview=')
        cmnd('fullUpdate', comment=" # Show hostInfo + servicesInfo + dagsInfo + usersInfo")

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
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print hostInfo, servicesInfo and usersInfo together.
        #+end_org """): return(cmndOutcome)

        self.captureRunStr(""" #+begin_org
#+begin_src sh :results output :session shared
  airflowAdmin.cs -i fullUpdate
#+end_src
#+RESULTS:
#+begin_example
#+end_example
        #+end_org """)

        hostInfo().pyCmnd()
        servicesInfo().pyCmnd()
        dagsInfo().pyCmnd()
        usersInfo().pyCmnd()

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
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print this host's Airflow fqdn/port and where DAGs/logs live.
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert

        cs.examples.menuSection('/Host/')
        literal(f"echo {_airflowFqdn}   # fqdn -- see py3/bin/airflow-here-dns.pcs")
        literal(f"echo {_airflowPortNu}   # webserver/api-server port -- see bisos.banna tcpPorts")
        literal(f"http://{_airflowFqdn}   # web UI -- open in a browser")

        cs.examples.menuSection('/AIRFLOW_HOME/')
        literal(f"echo {_airflowHome}")

        cs.examples.menuSection('/DAG files -- input DAGs directory for this installation/')
        literal(f"echo {_airflowDagsDir}   # AIRFLOW_HOME/dags -- drop .py DAG files here")
        literal(f"ls -la {_airflowDagsDir}")

        cs.examples.menuSection('/Logs/')
        literal(f"ls -la {_airflowLogsDir}")
        literal("journalctl -u airflow-webserver.service -f")
        literal("journalctl -u airflow-scheduler.service -f")
        literal("journalctl -u airflow-triggerer.service -f")

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
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Print links for operating the 4 airflow-*-sysd.pcs units
        (db-migration first, then webserver/scheduler/triggerer).
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert

        cs.examples.menuSection('/DB Migration (run/verify first)/')
        literal("py3/bin/airflow-db-sysd.pcs -i examples   # list this unit's Cmnds")
        literal("systemctl status airflow-db.service")

        cs.examples.menuSection('/Webserver/')
        literal("py3/bin/airflow-webserver-sysd.pcs -i examples")
        literal("systemctl status airflow-webserver.service")
        literal("systemctl restart airflow-webserver.service")

        cs.examples.menuSection('/Scheduler/')
        literal("py3/bin/airflow-scheduler-sysd.pcs -i examples")
        literal("systemctl status airflow-scheduler.service")
        literal("systemctl restart airflow-scheduler.service")

        cs.examples.menuSection('/Triggerer/')
        literal("py3/bin/airflow-triggerer-sysd.pcs -i examples")
        literal("systemctl status airflow-triggerer.service")
        literal("systemctl restart airflow-triggerer.service")

        cs.examples.menuSection('/CBS/CBM assembly (all 4 units + sbom + dns as one bundle)/')
        literal("py3/bin/airflow-cbs.pcs -i examples")
        literal("py3/bin/cbmProc-airflow.spcs -i examples")

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "usersInfo" :comment "" :extent "verify" :ro "cli" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<usersInfo>>  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class usersInfo(cs.Cmnd):
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
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Users and passwords under Airflow 3's SimpleAuthManager.

        NOTE: Airflow 3 has *no* ``airflow users`` command group. The Airflow 2
        commands (``users list`` / ``users create`` / ``users reset-password``)
        do not exist. Users are declared in configuration and passwords live in
        a plaintext JSON file under AIRFLOW_HOME.
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert
        _pwFile = _airflowHome / "simple_auth_manager_passwords.json.generated"

        cs.examples.menuSection('/Who The Users Are -- declared in config, not a DB/')
        literal(f"{_airflowCli} config get-value core auth_manager")
        literal(f"{_airflowCli} config get-value core simple_auth_manager_users"
                "   # username:role pairs -- NOT passwords")

        cs.examples.menuSection('/Current Password/')
        literal(f"sudo cat {_pwFile}")
        literal("airflow-sbom.pcs -i adminPasswd   # same value, read as root")

        cs.examples.menuSection('/Set The admin Password/')
        # SimpleAuthManager only fills in users MISSING from this file (see
        # simple_auth_manager.py, "if user.username not in passwords"), so a
        # hand-written entry survives restarts rather than being regenerated.
        literal(f"""echo '{{"admin": "airflow"}}' | sudo -u {_airflowUser} tee {_pwFile}""")
        literal(f"sudo -u {_airflowUser} chmod 600 {_pwFile}")
        literal("sudo systemctl restart airflow-webserver   # required, re-reads the file")

        cs.examples.menuSection('/Caution/')
        literal("# SimpleAuthManager is dev-only by design: plaintext passwords,")
        literal("# generated secrets printed to stdout/logs, no rotation mechanism.")
        literal("# Use FAB or Keycloak auth managers for anything exposed.")

        return cmndOutcome


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "dagsInfo" :extent "verify" :comment "DAGs -- list, trigger, inspect" :parsMand "" :parsOpt "" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<dagsInfo>>  *DAGs -- list, trigger, inspect*  =verify= ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class dagsInfo(cs.Cmnd):
    cmndParamsMandatory = [ ]
    cmndParamsOptional = [ ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
    ) -> b.op.Outcome:
        """DAGs -- list, trigger, inspect"""
        failed = b_io.eh.badOutcome
        callParamsDict = {}
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
####+END:
        if self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Listing, triggering and inspecting DAGs from the CLI.

        Every invocation is prefixed with ``sudo -u airflow env AIRFLOW_HOME=...``
        -- see the _airflowCli comment near the top of this file for why both
        halves are required.
        #+end_org """): return(cmndOutcome)

        literal = cs.examples.execInsert
        _dag = "<dag_id>"

        cs.examples.menuSection('/What Is Registered/')
        literal(f"{_airflowCli} dags list")
        literal(f"{_airflowCli} dags list-import-errors   # empty output is good")
        literal(f"{_airflowCli} dags list-runs {_dag}     # dag_id is POSITIONAL, not -d")

        cs.examples.menuSection('/Trigger A Run At Will/')
        literal(f"{_airflowCli} dags trigger {_dag}")
        literal(f"{_airflowCli} dags trigger {_dag} -r <run-id>   # label it, e.g. by config under test")
        literal("# A manual trigger QUEUES rather than starts when max_active_runs=1")
        literal("# and a scheduled run is already active.")

        cs.examples.menuSection('/Pause and Unpause/')
        literal(f"{_airflowCli} dags pause {_dag}")
        literal(f"{_airflowCli} dags unpause {_dag}")
        literal("# New DAGs land PAUSED. Nothing runs until unpaused.")

        cs.examples.menuSection('/Tasks Of A DAG/')
        literal(f"{_airflowCli} tasks list {_dag}")
        literal(f"{_airflowCli} tasks states-for-dag-run {_dag} <run_id>")
        literal(f"{_airflowCli} tasks test {_dag} <task_id>"
                "   # run ONE task now, no scheduling, no unpause needed")

        cs.examples.menuSection('/Graphical Dependencies/')
        literal("# The web UI Graph view is the usual answer: http://" + _airflowFqdn)
        literal(f"{_airflowCli} dags show {_dag}                  # needs the graphviz pip pkg")
        literal(f"{_airflowCli} dags show {_dag} --save /tmp/{_dag}.png")

        cs.examples.menuSection('/Health Of The Parsing Layer/')
        # Airflow 3 parses DAG files in a standalone dag-processor, not in the
        # scheduler. With it absent every unit looks healthy, the UI serves, and
        # no DAG is ever registered -- this is the check that distinguishes that
        # case from a working install.
        literal(f"{_airflowCli} jobs check --job-type DagProcessorJob")
        literal(f"{_airflowCli} jobs check --job-type SchedulerJob")

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
