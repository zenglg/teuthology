import logging
import re
import struct
from cStringIO import StringIO

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run

log = logging.getLogger(__name__)
systemd_cmd_templ = 'sudo systemctl {action} {daemon}@{id_}'


def get_systemd_cmd(action, daemon, id_):
    if daemon == 'rgw':
        daemon = 'radosgw'
        id_ = 'rgw.%s' % id_
    daemon = 'ceph-%s' % daemon
    cmd = systemd_cmd_templ.format(
        action=action,
        daemon=daemon,
        id_=id_,
    )
    return cmd


class DaemonState(object):
    """
    Daemon State.  A daemon exists for each instance of each role.
    """
    def __init__(self, remote, role, id_, *command_args, **command_kwargs):
        """
        Pass remote command information as parameters to remote site

        :param remote: Remote site
        :param role: Role (osd, rgw, mon, mds)
        :param id_: Id within role (osd.1, osd.2, for eaxmple)
        :param command_args: positional arguments (used in restart commands)
        :param command_kwargs: keyword arguments (used in restart commands)
        """
        self.remote = remote
        self.command_args = command_args
        self.command_kwargs = command_kwargs
        self.role = role
        self.type_ = self.role.split('.')[-1]
        self.id_ = id_
        self.log = command_kwargs.get('logger', log)
        self.proc = None

    @property
    def pid(self):
        raise NotImplementedError

    def stop(self, timeout=300):
        """
        Stop this daemon instance.

        Note: this can raise a CommandFailedError,
        CommandCrashedError, or ConnectionLostError.

        :param timeout: timeout to pass to orchestra.run.wait()
        """
        if not self.running():
            self.log.error('tried to stop a non-running daemon')
            return
        self.proc.stdin.close()
        self.log.debug('waiting for process to exit')
        try:
            run.wait([self.proc], timeout=timeout)
        except CommandFailedError:
            log.exception("Error while waiting for process to exit")
        self.proc = None
        self.log.info('Stopped')

    def start(self, timeout=300):
        """
        Start this daemon instance.
        """
        if self.running():
            self.log.warn('Restarting a running daemon')
        self.restart()

    def restart(self, *args, **kwargs):
        """
        Restart with a new command passed in the arguments

        :param args: positional arguments passed to remote.run
        :param kwargs: keyword arguments passed to remote.run
        """
        self.log.info('Restarting daemon')
        if self.proc is not None:
            self.log.info('Stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        cmd_args.extend(args)
        cmd_kwargs = self.command_kwargs
        cmd_kwargs.update(kwargs)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def restart_with_args(self, extra_args):
        """
        Restart, adding new paramaters to the current command.

        :param extra_args: Extra keyword arguments to be added.
        """
        self.log.info('Restarting daemon with args')
        if self.proc is not None:
            self.log.info('Stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        # we only want to make a temporary mod of the args list
        # so we shallow copy the dict, and deepcopy the args list
        cmd_kwargs = self.command_kwargs.copy()
        from copy import deepcopy
        cmd_kwargs['args'] = deepcopy(self.command_kwargs['args'])
        cmd_kwargs['args'].extend(extra_args)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def signal(self, sig, silent=False):
        """
        Send a signal to associated remote commnad

        :param sig: signal to send
        """
        self.proc.stdin.write(struct.pack('!b', sig))
        if not silent:
            self.log.info('Sent signal %d', sig)

    def running(self):
        """
        Are we running?
        :return: True if remote run command value is set, False otherwise.
        """
        return self.proc is not None

    def reset(self):
        """
        clear remote run command value.
        """
        self.proc = None

    # FIXME why are there two wait methods?
    def wait(self, timeout=300):
        """
        Wait for daemon to exit

        Wait for daemon to stop (but don't trigger the stop).  Pass up
        any exception.  Mark the daemon as not running.
        """
        self.log.debug('waiting for process to exit')
        try:
            run.wait([self.proc], timeout=timeout)
            self.log.info('Stopped')
        except:
            self.log.info('Failed')
            raise
        finally:
            self.proc = None

    def wait_for_exit(self):
        """
        clear remote run command value after waiting for exit.
        """
        if self.proc:
            try:
                run.wait([self.proc])
            finally:
                self.proc = None

    def check_status(self):
        """
        Check to see if the process has exited.

        :returns: The exit status, if any
        :raises:  CommandFailedError, if the process was run with
                  check_status=True
        """
        if self.proc:
            return self.proc.poll()


class SystemDState(DaemonState):
    def __init__(self, remote, role, id_, *command_args,
                 **command_kwargs):
        super(SystemDState, self).__init__(
                remote, role, id_, *command_args, **command_kwargs)
        self._set_commands()

    def _set_commands(self):
        self.start_cmd = get_systemd_cmd('start', self.type_, self.id_)
        self.stop_cmd = get_systemd_cmd('stop', self.type_, self.id_)
        self.restart_cmd = get_systemd_cmd('restart', self.type_, self.id_)
        self.show_cmd = get_systemd_cmd('show', self.type_, self.id_)
        self.status_cmd = get_systemd_cmd('status', self.type_, self.id_)
        self.output_cmd = 'sudo journalctl -u ' \
            '{role}@{id_} -t {role} -n 10'.format(
                role=self.role.replace('.', '-'), id_=self.id_,
            )

    @property
    def pid(self):
        proc_name = 'ceph-%s' % self.type_
        proc_regex = '"%s.*--id %s"' % (proc_name, self.id_)
        args = ['ps', '-ef',
                run.Raw('|'),
                'grep',
                run.Raw(proc_regex),
                run.Raw('|'),
                'grep', '-v',
                'grep', run.Raw('|'),
                'awk',
                run.Raw("{'print $2'}")]
        proc = self.remote.run(args=args, stdout=StringIO())
        pid_string = proc.stdout.getvalue().strip()
        if not pid_string.isdigit():
            return None
        return int(pid_string)

    def stop(self, timeout=300):
        """
        Stop this daemon instance.

        Note: this can raise a CommandFailedError,
        CommandCrashedError, or ConnectionLostError.

        :param timeout: timeout to pass to orchestra.run.wait()
        """
        if not self.running():
            self.log.error('tried to stop a non-running daemon')
            return
        self.log.info("using systemd to stop")
        self.remote.run(args=[run.Raw(self.stop_cmd)])
        self.log.info('Stopped')

    def start(self, timeout=300):
        """
        Start this daemon instance.
        """
        if self.running():
            self.log.warn('Restarting a running daemon')
            self.restart()
            return
        self.log.info("using systemd to start")
        self.remote.run(args=[run.Raw(self.start_cmd)])

    def restart(self, *args, **kwargs):
        """
        Restart with a new command passed in the arguments

        :param args: positional arguments passed to remote.run
        :param kwargs: keyword arguments passed to remote.run
        """
        self.log.info('Restarting daemon')
        self.log.info("using systemd to restart")
        if not self.running():
            self.log.info('starting a non-running daemon')
            self.remote.run(args=[run.Raw(self.start_cmd)])
        else:
            self.remote.run(args=[run.Raw(self.restart_cmd)])

    def restart_with_args(self, extra_args):
        """
        Restart, adding new paramaters to the current command.

        :param extra_args: Extra keyword arguments to be added.
        """
        self.log.info('Restarting daemon with args')
        self.log.warn("restart with args not supported with systemd")
        self.restart()

    def signal(self, sig, silent=False):
        """
        Send a signal to associated remote commnad

        :param sig: signal to send
        """
        self.log.info("using systemd to send signal")
        self.log.warn("systemd may restart daemon after kill signal")
        pid = self.pid
        self.log.info("Sending signal %s to process %s", sig, pid)
        sig = '-' + str(sig)
        self.remote.run(args=['sudo', 'kill', str(sig), pid])

    def running(self):
        """
        Are we running?
        :return: The PID if remote run command value is set, False otherwise.
        """
        pid = self.pid
        if pid > 0:
            return pid
        else:
            return None

    def reset(self):
        """
        Does nothing in this implementation
        """
        pass


    def check_status(self):
        """
        Check to see if the process has exited.

        :returns: The exit status, if any
        :raises:  CommandFailedError, if the process was run with
                  check_status=True
        """
        proc = self.remote.run(
            args=self.show_cmd + ' | grep -i state',
            stdout=StringIO(),
        )

        def parse_line(line):
            key, value = line.strip().split('=', 1)
            return {key.strip(): value.strip()}
        show_dict = dict()
        for line in proc.stdout.readlines():
            show_dict.update(parse_line(line))
        active_state = show_dict['ActiveState']
        sub_state = show_dict['SubState']
        if active_state == 'active':
            return None
        self.log.info("State is: %s/%s", active_state, sub_state)
        proc = self.remote.run(
            # This will match a line like:
            #    Main PID: 13394 (code=exited, status=1/FAILURE)
            # Or (this is wrapped):
            #    Apr 26 21:29:33 ovh083 systemd[1]: ceph-osd@1.service:
            #    Main process exited, code=exited, status=1/FAILURE
            args=self.status_cmd + " | grep 'Main.*code=exited'",
            stdout=StringIO(),
        )
        line = proc.stdout.readlines()[-1]
        exit_code = int(re.match('.*status=(\d+).*', line).groups()[0])
        if exit_code:
            self.remote.run(
                args=self.output_cmd
            )
            raise CommandFailedError(
                self.start_cmd,
                exit_code,
                self.remote,
            )
        return exit_code


    # FIXME why are there two wait methods?
    def wait(self, timeout=300):
        """
        Wait for daemon to exit

        Wait for daemon to stop (but don't trigger the stop).  Pass up
        any exception.  Mark the daemon as not running.
        """
        self.log.info("Wait not suported in systemd")

    def wait_for_exit(self):
        """
        clear remote run command value after waiting for exit.
        """
        # TODO: This ought to be possible, no?
        self.log.error("wait_for_exit is not supported with systemd")