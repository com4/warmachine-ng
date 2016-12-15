import asyncio
from datetime import datetime, timedelta
import functools
import json
import os
from pprint import pformat

from .base import WarMachinePlugin


class StandUpPlugin(WarMachinePlugin):
    """
    WarMachine stand up plugin.

    Commands:
        In a channel:
            !standup-add <24 hr time to kick off>
            !standup-remove
        Direct Message:
            !standup-ignore [users]
            !standup-schedules
            !standup-waiting_replies
    """
    SETTINGS_FILENAME = 'standup_schedules.json'
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 'CHANNEL': {
        #     'future': Task for running next standup,
        #     'time24': Original 24h time to schedule,
        #     'datetime': datetime object of when the next schedule will run,
        #     'ignoring': list of users to ignore when priv messaging,
        # }
        self.standup_schedules = {}

        # 'DM_CHANNEL': {
        #     'user': 'UID',
        #     'for_channels': ['CHID',],
        # }
        self.users_awaiting_reply = {}
        self.log.info('Loaded standup plugin')

        self.settings_file = os.path.join(
            self.config_dir, self.SETTINGS_FILENAME)

        if not os.path.exists(self.settings_file):
            self.log.info('Creating standup config file: {}'.format(
                self.settings_file))
            with open(self.settings_file, 'w') as f:
                f.write('{}')

    def on_connect(self, connection):
        self.load_schedule(connection)

    async def recv_msg(self, connection, message):
        """
        When the connection receives a message this method is called. We parse
        the message for commands we want to listen for.

        Args:
            connection (Connection): the warmachine connection object
            message (dict): the warmachine formatted message
        """
        if not message['message'].startswith('!standup') \
           and not message['channel'] \
           and message['sender'] in self.users_awaiting_reply:
            self.log.debug("Probable standup reply recvd from {}: {}".format(
                message['sender'], message['message']))

            user_nick = message['sender']

            data = self.users_awaiting_reply[user_nick]

            for_channels = data['for_channels']

            if 'pester_task' in data and data['pester_task']:
                self.log.debug('Stopping pester for {}'.format(user_nick))
                data['pester_task'].cancel()
                data['pester_task'] = None

            announce_message = '*@{}*: {}'.format(
                user_nick,
                message['message']
            )

            self.users_awaiting_reply[user_nick]['standup_msg'] = \
                message['message']

            f = self._loop.call_later(
                16*(60*60),  # 16 hours
                self.clear_old_standup_message_schedule_func, user_nick
            )

            self.users_awaiting_reply[user_nick]['clear_standup_msg_f'] = f

            for i in range(0, len(for_channels)):
                c = self.users_awaiting_reply[user_nick]['for_channels'].pop()
                await connection.say(announce_message, c)

            del data
            # del self.users_awaiting_reply[user_nick]
            return

        # Otherwise parse for the commands:

        cmd = message['message'].split(' ')[0]
        parts = message['message'].split(' ')[1:]
        channel = message['channel']
        user_nick = message['sender']

        # ======================================================================
        # !standup-add <24h time>
        #
        # Add (or update if one exists) a schedule for standup at the given 24h
        # time M-F
        # ======================================================================
        if cmd == '!standup-add' and channel:
            # If there is already a schedule, kill the task for the old one.
            if channel in self.standup_schedules:
                self.standup_schedules[channel]['future'].cancel()
                self.log.info('Unscheduling existing schedule for {} at '
                              '{}'.format(
                                  channel,
                                  self.standup_schedules[channel]['time24h']))

            self.schedule_standup(connection, channel, parts[0])
            self.save_schedule(connection)

        # ======================================================================
        # !standup-remove
        #
        # Remove an existing schedule from the channel
        # ======================================================================
        elif cmd == '!standup-remove' and channel:
            if channel in self.standup_schedules:
                self.standup_schedules[channel]['future'].cancel()
                del self.standup_schedules[channel]
                self.save_schedule(connection)
                self.log.info('Removed standup for channel {}'.format(channel))

        # ======================================================================
        # !standup-ignore
        # !standup-ignore <space seperated list of users to ignore>
        #
        # Ignore users provided when private messaging asking the standup
        # questions.
        # If no users are provided, display the users currently being ignored
        # ======================================================================
        elif cmd == '!standup-ignore' and channel \
             and channel in self.standup_schedules:  # noqa - indent level
            if parts:
                users_to_ignore = parts
                for u in users_to_ignore:
                    if u not in self.standup_schedules[channel]['ignoring']:
                        self.log.info('Ignoring {} in channel {}'.format(
                            u, channel))
                        self.standup_schedules[channel]['ignoring'].append(u)

                # Save the new users to ignore for this channel
                self.save_schedule(connection)

            ignoring = ', '.join(
                self.standup_schedules[channel]['ignoring'])
            if not ignoring:
                ignoring = 'no one'

            await connection.say('Currently ignoring {}'.format(ignoring),
                                 channel)

        # ======================================================================
        # !standup-unignore <space separated users>
        #
        # Remove users from the ignore list.
        # ======================================================================
        elif cmd == '!standup-unignore' and channel \
             and channel in self.standup_schedules:  # noqa - indent level
            removed_users = []  # Used for the message

            if parts:
                users_to_unignore = parts
                for u in users_to_unignore:
                    if u in self.standup_schedules[channel]['ignoring']:
                        self.log.info('Unignoring {} in channel {}'.format(
                            u, channel))
                        self.standup_schedules[channel]['ignoring'].remove(u)
                        removed_users.append(u)
                self.save_schedule(connection)

            if removed_users:
                await connection.say('Removed {} from the ignore list'.format(
                    ', '.join(removed_users)), channel)

            ignoring = ', '.join(
                self.standup_schedules[channel]['ignoring'])
            if not ignoring:
                ignoring = 'no one'

            await connection.say('Currently ignoring {}'.format(ignoring),
                                 channel)

        # ======================================================================
        # !standup-schedules
        #
        # Report the current standup schedule dict to the requesting user
        # ======================================================================
        elif not channel and cmd == '!standup-schedules':
            self.log.info('Reporting standup schedules to {}'.format(
                user_nick))
            await connection.say('Standup Schedules', user_nick)
            await connection.say('-----------------', user_nick)
            await connection.say(
                'Current Loop Time: {}'.format(self._loop.time()), user_nick)
            await connection.say(
                'Current Time: {}'.format(datetime.now()), user_nick)
            await connection.say(pformat(self.standup_schedules), user_nick)

        # ======================================================================
        # !standup-waiting_replies
        #
        # Report the data struct of users we are waiting on a reply from  to
        # the requesting user.
        # ======================================================================
        elif not channel and cmd == '!standup-waiting_replies':
            self.log.info('Reporting who we are waiting on replies for to '
                          ' {}'.format(user_nick))
            await connection.say('Waiting for Replies From', user_nick)
            await connection.say('------------------------', user_nick)
            await connection.say(
                pformat(self.users_awaiting_reply), user_nick)

    def schedule_standup(self, connection, channel, time24h):
        """
        Schedules a standup by creating a Task to be run in the future. This
        populates ``self.standup_schedules[channel]`` with the following keys:
         - ``future`` (:class:`asyncio.Task`): This is the asyncio task object.
         - ``datetime`` (:class:`datetime.datetime`): The datetime of when the
            standup will run next.
         - ``time24h`` (str): 24 hour time the schedule should be executed at.
         - ``ignoring`` (list): List of usernames to ignore when asking for
            their standup update.

        Args:
            connection (:class:`Connection`): the connection
            channel (str): channel name to schedule standup for
            time24h (str): The 24 hour time to start the standup at
        """
        next_standup = self.get_next_standup_secs(time24h)

        standup_td = next_standup - datetime.now()
        next_standup_secs = standup_td.seconds

        f = self._loop.call_later(
            next_standup_secs, functools.partial(
                self.standup_schedule_func, connection, channel))

        # Don't overwrite existing setting if they exist
        if channel in self.standup_schedules:
            self.standup_schedules[channel]['future'].cancel()

            self.standup_schedules[channel]['future'] = f
            self.standup_schedules[channel]['datetime'] = next_standup
            self.standup_schedules[channel]['time24h'] = time24h
        else:
            self.standup_schedules[channel] = {
                'future': f,
                'datetime': next_standup,
                'time24h': time24h,
                'ignoring': [],
            }

        self.log.info('New schedule added to channel {} for {}'.format(
            channel, time24h))

    def standup_schedule_func(self, connection, channel):
        """
        Non-async function used to schedule the standup for a channel.

        See :meth:`start_standup`
        """
        self.log.info('Executing standup for channel {}'.format(channel))
        asyncio.ensure_future(self.start_standup(connection, channel))

    def pester_schedule_func(self, connection, user, channel, pester,
                             pester_count=0):
        """
        Non-async function used to schedule pesters for a user.

        See :meth:`standup_priv_msg`
        """
        self.log.info('Pestering user {} to give a standup for channel '
                      '{} (interval: {}s)'.format(user, channel, pester))
        asyncio.ensure_future(self.standup_priv_msg(
            connection, user, channel, pester, pester_count))

    def clear_old_standup_message_schedule_func(self, user):
        """
        This function is scheduled to remove old standup messages so that the
        user is asked for updates on the next standup.
        """
        self.log.info('Clearing old standup message for {}'.format(user))
        del self.users_awaiting_reply[user]['clear_standup_msg_f']
        del self.users_awaiting_reply[user]['standup_msg']

    async def start_standup(self, connection, channel):
        """
        Notify the channel that the standup is about to begin, then loop
        through all the users in the channel asking them report their standup.
        """
        users = connection.get_users_by_channel(channel)
        self.log.debug('Users found in {}: {}'.format(channel, users))
        if not users:
            self.log.error('Unable to get_users_by_channel for channel '
                           '{}. Skipping standup.'.format(channel))
            return
        await connection.say('@channel Time for standup', channel)

        for u in users:
            if u == connection.nick or \
               u in self.standup_schedules[channel]['ignoring']:
                # try:
                #     users.remove(u)
                # except ValueError:
                #     pass
                continue

            if u in self.users_awaiting_reply and \
               'standup_msg' in self.users_awaiting_reply[u]:
                await connection.say('*@{}*: {}'.format(
                    u, self.users_awaiting_reply[u]['standup_msg']), channel)
            else:
                await self.standup_priv_msg(connection, u, channel)

        # schedule a function to run in 8 hours to clear out this channel from
        # self.users_awaiting_reply for all `users`.
        # This is assuming that after the hours, nobody cares about the report
        # from people who never reported earlier. It will prevent flooding
        # "tomorrow's" response to channels whose standup is scheduled for
        # later.
        self._loop.call_later(8*(60*60),  # 8 hours
                              self.clean_channel_from_waiting_replies, channel,
                              users)

        # Schedule the next standup
        self.schedule_standup(
            connection, channel, self.standup_schedules[channel]['time24h'])


    async def standup_priv_msg(self, connection, user, channel, pester=600,
                               pester_count=0):
        """
        Send a private message to ``user`` asking for their standup update.

        Args:
            connection (:class:`warmachine.base.Connection'): Connection object
                to use.
            user (str): User to send the message to.
            channel (str): The channel the standup is for
            pester (int): Number of seconds to wait until asking the user
                again. Use 0 to disable
            pester_count (int): An internal counter to stop pestering after
                awhile.
        """
        self.log.debug('Messaging user: {}'.format(user))

        if user in self.users_awaiting_reply:
            if 'standup_msg' in self.users_awaiting_reply[user]:
                await connection.say('*@{}*: {}'.format(
                    user, self.users_awaiting_reply[user]['standup_msg']
                ), channel)

                self.users_awaiting_reply[user]['pester_task'].cancel()
                self.users_awaiting_reply[user]['pester_task'] = None

                return
            # Don't readd an existing channel
            if channel not in self.users_awaiting_reply[user]['for_channels']:
                self.users_awaiting_reply[user]['for_channels'].append(channel)
        else:
            self.log.debug('Waiting user {} for a reply '.format(user))
            self.users_awaiting_reply[user] = {
                'for_channels': [channel, ],
            }

        # They are already being bothered about it. It won't help to bother them
        # for a different thing
        if pester_count == 0 and \
           'pester_task' in self.users_awaiting_reply[user] and \
           self.users_awaiting_reply[user]['pester_task']:
            return

        for_channels = self.users_awaiting_reply[user]['for_channels']
        await connection.say('What did you do yesterday? What will you '
                             'do today? do you have any blockers? '
                             '(standup for:{})'.format(
                                 ', '.join(for_channels)), user)

        if pester > 0 and pester_count <= 2:
            self.log.info('Scheduling pester for {} {}m from now'.format(
                user, pester/60))
            f = self._loop.call_later(
                pester, functools.partial(
                    self.pester_schedule_func, connection, user, channel,
                    pester, pester_count+1))
            self.users_awaiting_reply[user]['pester_task'] = f

    @classmethod
    def get_next_standup_secs(cls, time24h):
        """
        calculate the number of seconds until the next standup time

        Args:
            time24h (str): The 24 hour version of the time that the standup
            should run on Mon-Fri

        Returns:
            datetime: Datetime object representing the next datetime the
                standup will begin
        """
        now = datetime.now()

        standup_hour, standup_minute = (int(s) for s in time24h.split(':'))

        next_standup = datetime(now.year, now.month, now.day,
                                standup_hour, standup_minute)

        # If we've already past the time for today, schedule it for that time
        # on the next weekday
        if now > next_standup or now.isoweekday() > 4:
            # if it's friday(5), wait 72 hours
            if now.isoweekday() == 5:
                hours = 72
            # if it's saturday(6), wait 48
            elif now.isoweekday() == 6:
                hours = 48
            # if it's sunday(7)-thur(4) wait 24
            else:
                hours = 24

            future = now + timedelta(hours=hours)
            next_standup = datetime(future.year, future.month, future.day,
                                    standup_hour, standup_minute)

        return next_standup

    def clean_channel_from_waiting_replies(self, channel, users):
        """
        This clears ``channel`` from the list of interested channels for a
        user's stand up, so that when the next stand up comes and they answer,
        the other channels won't recieve information they are most likely not
        interested in anymore

        Args:
            channel (str): The channel to clear out
            users (list): List of users to check for
        """
        for u in users:
            if u in self.users_awaiting_reply:
                self.log.info('Clearing channel {} from list of waiting '
                              'channels for user {}'.format(channel, u))

                try:
                    self.users_awaiting_reply[u]['for_channels'].remove(channel)
                except ValueError as e:
                   pass

                # if that was the last channel, kill any pester tasks
                if not self.users_awaiting_reply[u]['for_channels'] and \
                   self.users_awaiting_reply[u]['pester_task']:
                    self.log.info('No more interested channels for {}. '
                                  'Cancelling pester.'.format(u))
                    self.users_awaiting_reply[u]['pester_task'].cancel()
                    del self.users_awaiting_reply[u]['pester_task']

    def save_schedule(self, connection):
        """
        Save all channel schedules to a file.
        """
        keys_to_save = ['time24h', 'ignoring']
        data = {}
        for channel in self.standup_schedules:
            data[channel] = {}
            for key in keys_to_save:
                data[channel][key] = self.standup_schedules[channel][key]

        data = {connection.id: data}
        with open(self.settings_file, 'w') as f:
            f.write(json.dumps(data))

        self.log.info('Schedules saved to disk')

    def load_schedule(self, connection):
        """
        Load the channel schedules from a file.
        """
        with open(self.settings_file, 'r') as f:
            try:
                data = json.loads(f.read())
            except Exception as e:
                self.log.debug('Error loading standup schedules: {}'.format(e))
                return

        if connection.id not in data:
            return

        for channel in data[connection.id]:
            self.schedule_standup(
                connection, channel, data[connection.id][channel]['time24h'])

            # Restore the ignore list
            try:
                self.standup_schedules[channel]['ignoring'] = \
                    data[connection.id][channel]['ignoring']
            except KeyError:
                pass
