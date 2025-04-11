import logging
import discord
import pandas as pd
from os import getenv
import aiosqlite as sql
from asyncio import sleep
from time import timezone
from dotenv import load_dotenv
from discord.ext import commands
from utils import parse_commands, get_active_polls, deactivate_polls, format_message, tally_votes

load_dotenv(override=True)
intents = discord.Intents.default()
intents.message_content = True
DATABASE_PATH = getenv('DATABASE_PATH')
TIMEZONE_OFFSET = timezone

logger = logging.getLogger(__name__)
logging.basicConfig(filename="process_data.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')

bot = commands.Bot(command_prefix='!', intents=intents)


@bot.command()
async def qpstart(ctx, *, arg):
    start_args = await parse_commands(command_string=arg, command_type='qpstart')
    active_polls = await get_active_polls(path=DATABASE_PATH, name=start_args.name)

    options_list = start_args.options
    poll_name = ' '.join(start_args.name)
    poll_question = ' '.join(start_args.question)
    starting_time = int(pd.to_datetime('now').timestamp()) + TIMEZONE_OFFSET
    time_difference = active_polls[0].get('ended_at') - starting_time if active_polls else 0

    logging.info(f'!qpstart args: {start_args}')

    if active_polls:
        start_msg = f'''
        Poll with the name {poll_name} already exists. Vote on that poll using the command:
        `!qpvote -n/--name <poll name> -o/--option <poll option> -i/--id <poll id, optional>`
        '''
        zombie_polls = [row.get('id') for row in active_polls if row.get('ended_at') <= starting_time]
        await deactivate_polls(DATABASE_PATH, name=poll_name, poll_id=zombie_polls, current_time=starting_time)
    else:
        time_difference = int(pd.to_timedelta(start_args.duration[0] or '5m').total_seconds()) # default 5 mins
        ending_time = starting_time + time_difference

        async with sql.connect(DATABASE_PATH) as con:
            await con.execute(
                '''
                INSERT INTO polls ("name", creator_id, question, choice_list, "status", created_at, ended_at)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                ''',
                (poll_name, ctx.author.id, ' '.join(start_args.question), ','.join(options_list), 'active', starting_time, ending_time)
            )
            await con.commit()
            logging.info(f'''Created new poll: {poll_name} and inserted {con.total_changes} rows into the polls table: {(poll_name, ctx.author.id, ' '.join(start_args.question), ','.join(options_list), 'active', starting_time, ending_time)}''')

        start_msg = f'''
        Poll `{poll_name}` started!
        Question: {poll_question}
        
        Options:
        {'\n'.join(f'{idx+1}. {ans}' for idx, ans in enumerate(options_list))}
        
        Vote using the command `!qpvote -n <poll name> -o 1-{len(options_list)}` or `!qpvote -n <poll name> -o <option>`
        Poll will close at <t:{ending_time}>
        '''

    # Send start_msg
    await ctx.send(await format_message(start_msg))

    # TODO: Figure out how to handle multiple !qpstart sessions
    # e.g. if 1 users type !qpstart <args> the bot will write 5 end_messages after the poll has ended
    # Idea: Make a request to a CountdownBot that keeps track of countdowns.
    # If the poll already has an associated countdown, just return `active_polls` message
    if time_difference > 0:
        # Awaiting end of poll
        await sleep(time_difference)

        col_width = len(max(options_list, key=len)) + 4

        logging.info(f'Formatting tallied votes for {poll_name} poll with column width: {col_width}')

        result_msg = await tally_votes(DATABASE_PATH, name=poll_name, spacing=col_width)
        end_message = f'''
        Poll `{poll_name}` as ended!
        Question: {poll_question}

        Results:
        {result_msg}
        '''

        # Delete old message and post results
        await ctx.send(await format_message(end_message))


@bot.command()
async def qpstop(ctx, *, arg):
    start_args = await parse_commands(command_string=arg, command_type='qpstop')
    poll_name = ' '.join(start_args.name)
    active_polls = await get_active_polls(DATABASE_PATH, poll_name)
    user_id = ctx.author.id

    logging.info(f'!qpstop args: {start_args}')

    if active_polls:
        user_owned_polls = [{'id': row.get('id'), 'question': row.get('question'), 'options': row.get('choice_list')} for row in active_polls if row.get('creator_id') == user_id][0]
        if user_owned_polls:
            await deactivate_polls(
                DATABASE_PATH,
                name=poll_name,
                poll_id=user_owned_polls.get('id'),
                current_time=int(pd.to_datetime('now').timestamp()) + TIMEZONE_OFFSET
            )

            col_width = len(max(user_owned_polls.get('options').split(','), key=len)) + 4
            result_msg = await tally_votes(DATABASE_PATH, name=poll_name, poll_id=user_owned_polls.get('id'), spacing=col_width)
            poll_question = user_owned_polls.get('question') # Just get the first poll, one poll name per user

            stop_msg = f'''
            Poll `{poll_name}` as ended!
            Question: {poll_question}

            Results:
            {result_msg}
            '''
            logging.info(f'Stopped poll {poll_name}')
        else:
            stop_msg = f'{ctx.author.mention} Permission denied. you are not the creator of the poll, wait until the poll ends.'
            logging.warning(f'Current user {user_id} is not the creator of the currently active poll {user_owned_polls.get("id")}')
    else:
        stop_msg = '''
        No active polls, start one using the command:
        `!qpstart -q/--question <question text> -d/--duration <duration format> -n/--name <poll name> -o/--options: snake_choice1 camelChoice2, ...`
        '''
        logging.info('No active polls')

    await ctx.send(await format_message(stop_msg))


@bot.command()
async def qpvote(ctx, *, arg):
    start_args = await parse_commands(command_string=arg, command_type='qpvote')
    poll_name = ' '.join(start_args.name)
    user_vote = ' '.join(start_args.option)
    active_polls = await get_active_polls(DATABASE_PATH, poll_name)
    current_time = int(pd.to_datetime('now').timestamp()) + TIMEZONE_OFFSET

    logging.info(f'Current time: {current_time}')
    logging.info(f'!qpvote args: {start_args}')


    if active_polls:
        logging.info(f'All active polls: {active_polls}')
        zombie_polls = [row.get('id') for row in active_polls if row.get('ended_at') <= current_time]
        active_choices = [row for row in active_polls if row.get('ended_at') > current_time]
        allowed_choice = active_polls[0].get('choice_list').split(',') if len(active_polls) > 0 else None

        if user_vote.isnumeric():
            user_vote = allowed_choice[int(user_vote)-1]

        if zombie_polls: # Close polls that should have closed previously
            logging.info(f'Cosing polls: {zombie_polls}')
            await deactivate_polls(DATABASE_PATH, name=poll_name, poll_id=zombie_polls, current_time=current_time)

        if len(active_choices) == 0:
            vote_msg = '''
            No active polls, start one using the command:
            `!qpstart -q/--question <question text> -d/--duration <duration format> -n/--name <poll name> -o/--options: snake_choice1 camelChoice2, ...`
            '''

        elif allowed_choice and user_vote not in allowed_choice:
            vote_msg = f'''
            {start_args.option} is not in {allowed_choice}, choose one of the options in the poll.
            '''

        else:
            async with sql.connect(DATABASE_PATH) as con:
                try:
                    await con.execute(
                        '''
                        INSERT INTO votes (poll_id, member_id, vote_answer, voted_at)
                        VALUES (?, ?, ?, ?);
                        ''',
                        (active_choices[0].get('id'), ctx.author.id, user_vote, int(pd.to_datetime('now').timestamp()))
                    )
                    await con.commit()

                    vote_msg = f'''
                    Thank you for voting {ctx.author.mention}
                    Stay tuned, the results will be announced at <t:{active_choices[0].get('ended_at')}>
                    '''
                    logging.info(f'User {ctx.author.nick or ctx.author.name} voted for {user_vote} in the poll `{poll_name}`')

                except sql.IntegrityError as err:
                    vote_msg = f'''
                    {ctx.author.mention}, can't vote more than once. Please wait untill the poll ends, or create a new poll using the command:
                    `!qpstart -q/--question <question text> -d/--duration <duration format> -n/--name <poll name> -o/--options: snake_choice1 camelChoice2, ...`
                    '''

    else:
        vote_msg = 'No active polls, start one using the command `!qpstart q: <text>; d: <duration format>; c: (choice1, choice2, ...);`'
        logging.info('No active polls')

    await ctx.send(await format_message(vote_msg))


@bot.command()
async def qplist(ctx, * arg):
    start_args = await parse_commands(command_string=arg, command_type='qplist')
    poll_name = ' '.join(start_args)
    active_polls = await get_active_polls(DATABASE_PATH, name=poll_name)

    if active_polls:
        sep_string = ''.join(['\n', '='*30, '\n'])
        header = 'Available polls:'
        body = [
            f'''
            Question: {row.get('question')}
            
            Options:
            {'\n'.join(f'{idx+1}. {ans}' for idx, ans in enumerate(row.get('choice_list').split(',')))}
            
            Vote using the command !qpvote -n {row.get('name')} -o <option>
            Poll will close at <t:{row.get('ended_at')}>
            '''
            for row in active_polls
        ]

        polls_msg = sep_string.join([header, *body])
    else:
        polls_msg = '''
        No active polls, start one using the command:
        `!qpstart -q/--question <question text> -d/--duration <duration format> -n/--name <poll name> -o/--options: snake_choice1 camelChoice2, ...`
        '''

    await ctx.send(await format_message(polls_msg))


@bot.event
async def on_command_error(ctx, error):
    err_message = None
    if isinstance(error, (commands.MissingRequiredArgument, commands.CommandInvokeError)):
        qp_command = ctx.message.clean_content.split(' ', maxsplit=1)[0]

        if qp_command == '!qpstart':
            err_message = 'Use the command:\n`!qpstart -q/--question <question text> -d/--duration <duration text> -n/--name <poll name> -o/-options <space seperated options>`'
            logging.info(f'qpstart error: {error}')
        elif qp_command == '!qpstop':
            err_message = 'Use the command:\n`!qpstop -n/--name <text> -i/--id <poll id, optional>`'
            logging.info(f'qpstop error: {error}')
        elif qp_command == '!qpvote':
            err_message = 'Use the command:\n`!qpvote -n/--name <name of poll> -o/--option <chosen vote option> -i/--id <poll id, optional>`'
            logging.info(f'qpvote error: {error}')
        elif qp_command == '!qplist':
            err_message = 'Use the command:\n`!qplist -n/--name <name of poll>, -i/--id <poll id, optional>`'
            logging.info(f'qplist error: {error}')
        else:
            logging.info(f'other error: {error}')

        if err_message:
            err_message = f'{err_message}\nFor more info run the command `!qphelp {qp_command[1:]}`'
        else:
            err_message = 'Unknown error.'

        await ctx.send(await format_message(err_message))


if __name__ == '__main__':
    bot.run(getenv('BOT_TOKEN'))