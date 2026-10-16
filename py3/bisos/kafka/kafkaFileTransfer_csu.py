# -*- coding: utf-8 -*-

""" #+begin_org
* ~[Summary]~ :: The CS commands =fileSend= and =fileReceive= over [[file:kafkaFileTransfer.py][kafkaFileTransfer.py]]:
send files through Kafka as headed records (one transaction per file), and optionally rebuild them on
the other side. Loaded by two thin commands, =kafkaFileSend.cs= and =kafkaFileReceive.cs=, each of which
shows only its own examples. Nothing here knows any particular consumer: every consumer-specific value
is a parameter. Design: =AI-WorkPlan.org= Stage 2, "The concept".
#+end_org """

####+BEGIN: b:py3:cs:file/dblockControls :classification "cs-lib"
""" #+begin_org
* [[elisp:(org-cycle)][| /Control Parameters Of This File/ |]] :: dblk ctrls classifications=cs-lib
#+BEGIN_SRC emacs-lisp
(setq-local b:dblockControls t) ; (setq-local b:dblockControls nil)
(put 'b:dblockControls 'py3:cs:Classification "cs-lib") ; one of cs-mu, cs-u, cs-lib, bpf-lib, pyLibPure
#+END_SRC
#+RESULTS:
: cs-lib
#+end_org """
####+END:

if 'csInfo' not in globals(): import typing ; csInfo: typing.Dict[str, typing.Any] = { 'moduleName': ['kafkaFileTransfer_csu'], }
csInfo['version'] = '202610140001'
csInfo['status']  = 'inDev'

import collections
import pathlib
import signal
import socket
import typing

from bisos import b
from bisos.b import cs
from bisos.b import b_io
from bisos.common import csParam

from bisos.kafka import kafkaFileTransfer as kft


####+BEGIN: blee:bxPanel:foldingSection :outLevel 0 :sep nil :title "Parameters" :anchor ""  :extraInfo "Common parameters of both commands"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  [[elisp:(outline-show-subtree+toggle)][| _Parameters_: |]]  Common parameters of both commands  [[elisp:(org-shifttab)][<)]] E|
#+end_org """
####+END:

def _par(csParams: cs.param.CmndParamDict, name: str, description: str, choices: typing.Optional[list] = None) -> None:
    csParams.parDictAdd(
        parName=name,
        parDescription=description,
        parDataType=None,
        parDefault=None,
        parChoices=choices if choices is not None else ["any"],
        argparseShortOpt=None,
        argparseLongOpt=f'--{name}',
    )


def commonParamsSpecify(csParams: cs.param.CmndParamDict) -> None:
    """Both commands: nothing here is specific to any consumer."""
    _par(csParams, 'bootstrap', "Kafka bootstrap servers, host:port[,host:port].")
    _par(csParams, 'topic', "Topic (receive: a comma-separated list).")
    _par(csParams, 'clientConf', "File of librdkafka settings, one name=value per line (security: sasl.*, ssl.*).")
    # send
    _par(csParams, 'msgKey', "Send: record key (e.g. the producing host). One file stays in one partition.")
    _par(csParams, 'headers', "Send: headers for every record, name=value[,name=value...].")
    _par(csParams, 'idHeaders', "Send: names of the headers that identify the file (default: all of --headers).")
    _par(csParams, 'attrs', "Send: free name=value[,...] carried in the BOF, for the receiver's --pathTemplate.")
    _par(csParams, 'model', "Send: files (records plus kfx-bof/kfx-eof, the default) or records (plain headed rows).",
         ["files", "records"])
    _par(csParams, 'eol', "Send: the file's line terminator: lf (default), crlf or cr.", ["lf", "crlf", "cr"])
    _par(csParams, 'root', "Send: the logical path of each file is its path relative to this directory "
                           "(default: the file name alone).")
    # receive
    _par(csParams, 'group', "Receive: consumer group id.")
    _par(csParams, 'outBase', "Receive: directory under which files are rebuilt.")
    _par(csParams, 'pathTemplate', "Receive: place files at this template instead of the sent path, "
                                   "e.g. '{vendor}/{name}'. Fields: the sender's attrs, {path}, {dir}, {name}.")
    _par(csParams, 'onExists', "Receive: path exists with other content: replace (default), fail or skip.",
         ["replace", "fail", "skip"])
    _par(csParams, 'latest', "Receive: also point symlink NAME, beside each file's directory, at the newest one.")
    _par(csParams, 'idleExit', "Receive: exit after this many seconds with nothing to read (default: run until stopped).")
    _par(csParams, 'maxFiles', "Receive: exit after this many files.")


####+BEGIN: blee:bxPanel:foldingSection :outLevel 0 :sep nil :title "Examples" :anchor ""  :extraInfo "Examples of both commands"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  [[elisp:(outline-show-subtree+toggle)][| _Examples_: |]]  Examples of both commands  [[elisp:(org-shifttab)][<)]] E|
#+end_org """
####+END:

# The "Try it" pair: a small file every Debian host has, sent to this host's broker and rebuilt
# under /tmp. Runs as shown, in either order (the receiver starts at the earliest offset; the
# broker creates the topic on first send). Nothing in it names any consumer.
demoFile = pathlib.Path("/etc/os-release")
demoTopic = "kafkaFileDemo"
demoGroup = "kafkaFileDemo"
demoOutBase = pathlib.Path("/tmp/kafkaFileDemo")
demoIdleExit = "10"


def _demoBootstrap() -> str:
    from bisos.kafka.kafkaInfo import kafkaInfo    # this host's broker, port from bisos.banna
    return kafkaInfo.bootstrapServers


def examples_tryIt(side: str) -> None:
    """The runnable demo pair, shown first by both commands. side is "send" or "receive"."""
    od = collections.OrderedDict
    cmnd = cs.examples.cmndEnter
    literal = cs.examples.execInsert
    bootstrap = _demoBootstrap()

    cs.examples.menuChapter(f'*Try it* -- send {demoFile} to this host\'s Kafka, rebuild it under {demoOutBase}')
    sendPars = od([('bootstrap', bootstrap), ('topic', demoTopic), ('headers', f'host={socket.gethostname()}')])
    receivePars = od([('bootstrap', bootstrap), ('topic', demoTopic), ('group', demoGroup),
                      ('outBase', str(demoOutBase)), ('idleExit', demoIdleExit)])
    if side == "send":
        cmnd('fileSend', pars=sendPars, args=str(demoFile),
             comment=" # one transaction; then run kafkaFileReceive.cs (its first example)")
        literal(f"kafkaFileReceive.cs -i fileReceive --bootstrap={bootstrap} --topic={demoTopic} --group={demoGroup} "
                f"--outBase={demoOutBase} --idleExit={demoIdleExit}")
    else:
        literal(f"kafkaFileSend.cs -i fileSend --bootstrap={bootstrap} --topic={demoTopic} "
                f"--headers=host={socket.gethostname()} {demoFile}")
        cmnd('fileReceive', pars=receivePars,
             comment=f" # exits after {demoIdleExit} s idle; again with this group: nothing new to read")
    literal(f"cmp {demoFile} {demoOutBase / demoFile.name} && echo identical   # a resend of it is skipped as a duplicate")
    literal(f"rm -r {demoOutBase}")


def examples_send() -> None:
    """Examples of fileSend, for kafkaFileSend.cs. Placeholders (<...>) are to be replaced."""
    od = collections.OrderedDict
    cmnd = cs.examples.cmndEnter

    examples_tryIt("send")

    cs.examples.menuChapter('*fileSend* -- one transaction per file: headed records, plus kfx-bof / kfx-eof')
    cmnd('fileSend',
         pars=od([('bootstrap', '127.0.0.1:24007'), ('topic', '<topic>'),
                  ('headers', 'run=<run>,host=<host>')]),
         args="<file>",
         comment=" # files model: the receiver may rebuild the file; every record carries the headers")
    cmnd('fileSend',
         pars=od([('bootstrap', '127.0.0.1:24007'), ('topic', '<topic>'), ('msgKey', '<host>'),
                  ('headers', 'run=<run>,host=<host>'), ('idHeaders', 'run,host'),
                  ('attrs', 'kind=<kind>'), ('root', '<dir>')]),
         args="<dir>/<sub>/<file> <dir>/<sub>/<file2>",
         comment=" # the logical path of each file is relative to --root: the hierarchy can be reproduced")
    cmnd('fileSend',
         pars=od([('bootstrap', '127.0.0.1:24007'), ('topic', '<topic>'), ('model', 'records'),
                  ('headers', 'run=<run>,host=<host>')]),
         args="<file>",
         comment=" # records model: plain headed rows, no markers (consumers that ignore files)")
    cmnd('fileSend',
         pars=od([('bootstrap', '<host>:9093'), ('topic', '<topic>'), ('headers', 'run=<run>'),
                  ('clientConf', '~/.config/kafka/<cluster>.properties')]),
         args="<file>",
         comment=" # security (SASL_SSL, GSSAPI, ...) comes from a librdkafka settings file")


def examples_receive() -> None:
    """Examples of fileReceive, for kafkaFileReceive.cs. Placeholders (<...>) are to be replaced."""
    od = collections.OrderedDict
    cmnd = cs.examples.cmndEnter

    examples_tryIt("receive")

    cs.examples.menuChapter('*fileReceive* -- rebuild the files that fileSend sent (read_committed)')
    cmnd('fileReceive',
         pars=od([('bootstrap', '127.0.0.1:24007'), ('topic', '<topic>'), ('group', '<group>'),
                  ('outBase', '<dir>')]),
         comment=" # runs until stopped; the sent logical paths are reproduced under --outBase")
    cmnd('fileReceive',
         pars=od([('bootstrap', '127.0.0.1:24007'), ('topic', '<topic>'), ('group', '<group>'),
                  ('outBase', '<dir>'), ('idleExit', '30')]),
         comment=" # exits after 30 s with nothing to read (a batch run, a test)")
    cmnd('fileReceive',
         pars=od([('bootstrap', '127.0.0.1:24007'), ('topic', '<topic>'), ('group', '<group>'),
                  ('outBase', '<dir>'), ('pathTemplate', '{kind}/{name}'), ('latest', 'latest')]),
         comment=" # place by the sender's attrs instead; 'latest' follows the newest directory")


####+BEGIN: blee:bxPanel:foldingSection :outLevel 0 :sep nil :title "CmndSvcs" :anchor ""  :extraInfo "Command Services Section"
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  [[elisp:(outline-show-subtree+toggle)][| _CmndSvcs_: |]]  Command Services Section  [[elisp:(org-shifttab)][<)]] E|
#+end_org """
####+END:

def _int(name: str, value: typing.Optional[str]) -> typing.Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError:
        raise kft.FileTransferError(f"--{name} must be a number, not {value!r}")


def _failExit(failed: typing.Any, cmndOutcome: b.op.Outcome, why: str) -> b.op.Outcome:
    """Report the failure the PyCS way, then make the process exit status non-zero.

    The PyCS main discards a Cmnd's outcome (its source says exit codes are still to be cleaned up), so a
    failed Cmnd would otherwise exit 0 and a calling driver could not tell. Not a framework change: this
    only ends our own process after the failure has been reported.
    """
    failed(cmndOutcome, why)
    raise SystemExit(1)


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "fileSend" :comment "Send files through Kafka as headed records" :extent "verify" :ro "cli" :parsMand "bootstrap topic" :parsOpt "msgKey headers idHeaders attrs model eol root clientConf" :argsMin 1 :argsMax 9999 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<fileSend>>  =verify= parsMand=bootstrap topic parsOpt=msgKey headers idHeaders attrs model eol root clientConf argsMin=1 argsMax=9999 ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class fileSend(cs.Cmnd):
    cmndParamsMandatory = [ 'bootstrap', 'topic', ]
    cmndParamsOptional = [ 'msgKey', 'headers', 'idHeaders', 'attrs', 'model', 'eol', 'root', 'clientConf', ]
    cmndArgsLen = {'Min': 1, 'Max': 9999,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
             bootstrap: typing.Optional[str]=None,  # Cs Mandatory Param
             topic: typing.Optional[str]=None,  # Cs Mandatory Param
             msgKey: typing.Optional[str]=None,  # Cs Optional Param
             headers: typing.Optional[str]=None,  # Cs Optional Param
             idHeaders: typing.Optional[str]=None,  # Cs Optional Param
             attrs: typing.Optional[str]=None,  # Cs Optional Param
             model: typing.Optional[str]=None,  # Cs Optional Param
             eol: typing.Optional[str]=None,  # Cs Optional Param
             root: typing.Optional[str]=None,  # Cs Optional Param
             clientConf: typing.Optional[str]=None,  # Cs Optional Param
             argsList: typing.Optional[list[str]]=None,  # CsArgs
    ) -> b.op.Outcome:

        failed = b_io.eh.badOutcome
        callParamsDict = {'bootstrap': bootstrap, 'topic': topic, 'msgKey': msgKey, 'headers': headers, 'idHeaders': idHeaders, 'attrs': attrs, 'model': model, 'eol': eol, 'root': root, 'clientConf': clientConf, }
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, argsList).isProblematic():
            return failed(cmndOutcome)
        cmndArgsSpecDict = self.cmndArgsSpec()
        bootstrap = csParam.mappedValue('bootstrap', bootstrap)
        topic = csParam.mappedValue('topic', topic)
        msgKey = csParam.mappedValue('msgKey', msgKey)
        headers = csParam.mappedValue('headers', headers)
        idHeaders = csParam.mappedValue('idHeaders', idHeaders)
        attrs = csParam.mappedValue('attrs', attrs)
        model = csParam.mappedValue('model', model)
        eol = csParam.mappedValue('eol', eol)
        root = csParam.mappedValue('root', root)
        clientConf = csParam.mappedValue('clientConf', clientConf)
####+END:
        self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Send each file argument through Kafka: one transaction per file, one record per line
(value = the line without its terminator), every record carrying =--headers= and =--msgKey=.
With =--model files= (the default) the first record also carries =kfx-bof= and the last =kfx-eof=, so a
receiver may rebuild the file; a consumer that ignores files just sees headed rows. A file that cannot
be sent is aborted, so a =read_committed= reader never sees part of it; the command then fails overall.
        #+end_org """)

        try:
            filesMode = (model or "files") == "files"
            headerPairs = kft.parseKeyValues(headers)
            idNames = kft.parseNames(idHeaders) if idHeaders else [name for name, _ in headerPairs]
            if filesMode and not idNames:
                return _failExit(failed, cmndOutcome, "files model needs at least one header to identify each file: add --headers")
            rootDir = pathlib.Path(root).expanduser().resolve() if root else None
            toSend: list = []
            for each in (argsList or []):
                path = pathlib.Path(each).expanduser()
                if not path.is_file():
                    return _failExit(failed, cmndOutcome, f"not a file: {each}")
                if rootDir is not None:
                    try:
                        rel = path.resolve().relative_to(rootDir).as_posix()
                    except ValueError:
                        return _failExit(failed, cmndOutcome, f"{each} is not under --root {rootDir}")
                else:
                    rel = path.name
                toSend.append((path, rel))
            results = kft.sendFiles(
                toSend, bootstrap, topic, key=msgKey, headers=headerPairs,
                attrs=dict(kft.parseKeyValues(attrs)), idHeaders=idNames, eol=kft.eolOf(eol),
                filesMode=filesMode, clientConf=kft.readClientConf(clientConf),
                say=lambda line: b_io.ann.note(line),
            )
        except kft.FileTransferError as problem:
            return _failExit(failed, cmndOutcome, str(problem))

        bad = [f"{rel}: {why}" for rel, why in results if why != "sent"]
        if bad:
            return _failExit(failed, cmndOutcome, f"{len(bad)} of {len(results)} files not sent: " + "; ".join(bad))
        return cmndOutcome.set(opError=b.op.OpError.Success,
                               opResults=f"sent {len(results)} file(s) to {topic}")

    def cmndArgsSpec(self):
        cmndArgsSpecDict = cs.CmndArgsSpecDict()
        cmndArgsSpecDict.argsDictAdd(
            argPosition="0&-1",
            argName="filePaths",
            argChoices=[],
            argDescription="One or more files to send",
        )
        return cmndArgsSpecDict


####+BEGIN: b:py3:cs:cmnd/classHead :cmndName "fileReceive" :comment "Rebuild files from Kafka records" :extent "verify" :ro "cli" :parsMand "bootstrap topic group outBase" :parsOpt "pathTemplate onExists latest idleExit maxFiles clientConf" :argsMin 0 :argsMax 0 :pyInv ""
""" #+begin_org
*  _[[elisp:(blee:menu-sel:outline:popupMenu)][±]]_ _[[elisp:(blee:menu-sel:navigation:popupMenu)][Ξ]]_ [[elisp:(outline-show-branches+toggle)][|=]] [[elisp:(bx:orgm:indirectBufOther)][|>]] *[[elisp:(blee:ppmm:org-mode-toggle)][|N]]*  CmndSvc-   [[elisp:(outline-show-subtree+toggle)][||]] <<fileReceive>>  =verify= parsMand=bootstrap topic group outBase parsOpt=pathTemplate onExists latest idleExit maxFiles clientConf ro=cli   [[elisp:(org-cycle)][| ]]
#+end_org """
class fileReceive(cs.Cmnd):
    cmndParamsMandatory = [ 'bootstrap', 'topic', 'group', 'outBase', ]
    cmndParamsOptional = [ 'pathTemplate', 'onExists', 'latest', 'idleExit', 'maxFiles', 'clientConf', ]
    cmndArgsLen = {'Min': 0, 'Max': 0,}

    @cs.track(fnLoc=True, fnEntry=True, fnExit=True)
    def cmnd(self,
             rtInv: cs.RtInvoker,
             cmndOutcome: b.op.Outcome,
             bootstrap: typing.Optional[str]=None,  # Cs Mandatory Param
             topic: typing.Optional[str]=None,  # Cs Mandatory Param
             group: typing.Optional[str]=None,  # Cs Mandatory Param
             outBase: typing.Optional[str]=None,  # Cs Mandatory Param
             pathTemplate: typing.Optional[str]=None,  # Cs Optional Param
             onExists: typing.Optional[str]=None,  # Cs Optional Param
             latest: typing.Optional[str]=None,  # Cs Optional Param
             idleExit: typing.Optional[str]=None,  # Cs Optional Param
             maxFiles: typing.Optional[str]=None,  # Cs Optional Param
             clientConf: typing.Optional[str]=None,  # Cs Optional Param
    ) -> b.op.Outcome:

        failed = b_io.eh.badOutcome
        callParamsDict = {'bootstrap': bootstrap, 'topic': topic, 'group': group, 'outBase': outBase, 'pathTemplate': pathTemplate, 'onExists': onExists, 'latest': latest, 'idleExit': idleExit, 'maxFiles': maxFiles, 'clientConf': clientConf, }
        if self.invocationValidate(rtInv, cmndOutcome, callParamsDict, None).isProblematic():
            return failed(cmndOutcome)
        bootstrap = csParam.mappedValue('bootstrap', bootstrap)
        topic = csParam.mappedValue('topic', topic)
        group = csParam.mappedValue('group', group)
        outBase = csParam.mappedValue('outBase', outBase)
        pathTemplate = csParam.mappedValue('pathTemplate', pathTemplate)
        onExists = csParam.mappedValue('onExists', onExists)
        latest = csParam.mappedValue('latest', latest)
        idleExit = csParam.mappedValue('idleExit', idleExit)
        maxFiles = csParam.mappedValue('maxFiles', maxFiles)
        clientConf = csParam.mappedValue('clientConf', clientConf)
####+END:
        self.cmndDocStr(f""" #+begin_org
** [[elisp:(org-cycle)][| *CmndDesc:* | ]]  Read the topic(s) as a =read_committed= consumer and rebuild every file that
=fileSend --model files= sent: buffer per file, verify rows, bytes and SHA-256 against both markers,
write atomically under =--outBase=. Same path and content: skipped. Records without file markers are
counted, not written. Runs until stopped (SIGINT or SIGTERM), =--idleExit= seconds of silence, or
=--maxFiles= files. Offsets are committed only up to the oldest unfinished file.
        #+end_org """)

        stopRequested = {"flag": False}

        def _stop(_signum: int, _frame: typing.Any) -> None:
            stopRequested["flag"] = True

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        try:
            idle = _int('idleExit', idleExit)
            counts = kft.receiveFiles(
                bootstrap, kft.parseNames(topic), group, pathlib.Path(outBase),
                pathTemplate=pathTemplate, onExists=onExists or "replace", latest=latest,
                clientConf=kft.readClientConf(clientConf),
                idleExitSec=float(idle) if idle is not None else None, maxFiles=_int('maxFiles', maxFiles),
                say=lambda line: b_io.ann.note(line), stop=lambda: stopRequested["flag"],
            )
        except kft.FileTransferError as problem:
            return _failExit(failed, cmndOutcome, str(problem))

        summary = ", ".join(f"{name}={count}" for name, count in counts.items())
        if counts["orphans"]:
            b_io.ann.note(f"{counts['orphans']} record(s) carried no file markers (sent with --model records?); not written")
        if counts["failed"]:
            return _failExit(failed, cmndOutcome, f"some files were rejected: {summary}")
        return cmndOutcome.set(opError=b.op.OpError.Success, opResults=summary)


####+BEGIN: b:py3:cs:framework/endOfFile :basedOn "classification"
""" #+begin_org
* [[elisp:(org-cycle)][| *End-Of-Editable-Text* |]] :: emacs and org variables and control parameters
#+end_org """

#+STARTUP: showall

### local variables:
### no-byte-compile: t
### end:
####+END:
