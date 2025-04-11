import json
import shlex
import logging
import argparse
import aiosqlite as sql
from typing import Union

logger = logging.getLogger(__name__)
logging.basicConfig(filename="process_data.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Argsparser custom string
def nullable_string(val):
    if not val:
        return None
    return val

# Format multiline messages to strip leading line whitespace, textwrap dedent doesn't work
async def format_message(msg_string:str):
    return '\n'.join(line.lstrip() if line != '\n' else line for line in msg_string.splitlines())


# Parsing args inputs
async def parse_commands(command_string:str, command_type:str):
    parser = argparse.ArgumentParser(exit_on_error=False)
    if command_type == 'qpstart':
        parser.add_argument('-q', '--question', help='Question asked by the poll', nargs='*', required=True)
        parser.add_argument('-d', '--duration', help='Duration of the poll', nargs='*', default='5m')
        parser.add_argument('-n', '--name', help='Name of the poll', nargs='*', default=None, type=nullable_string)
        parser.add_argument('-o', '--options', help='List of options available to vote on', nargs='*', type=nullable_string)
    elif command_type == 'qpstop':
        parser.add_argument('-n', '--name', help='Name of the poll', nargs='*', required=True)
        parser.add_argument('-i', '--id', help='Id of the poll', default=None, type=nullable_string)
    elif command_type == 'qpvote':
        parser.add_argument('-n', '--name', help='Name of the poll', nargs='*', required=True)
        parser.add_argument('-o', '--option', help='Chosen user voting option', nargs='*', required=True)
        parser.add_argument('-i', '--id', help='Id of the poll', default=None, type=nullable_string)
    elif command_type == 'qplist':
        parser.add_argument('-n', '--name', help='Name of the poll', nargs='*', required=True)
        parser.add_argument('-i', '--id', help='Id of the poll', default=None, type=nullable_string)
    else:
        raise ValueError(f'no such command type {command_string}')

    logging.info(f'Parsed {command_type} arguments: "{command_string}"')

    return parser.parse_args(shlex.split(command_string))

# Deactivate all active polls
async def deactivate_polls(path:str, name:str, poll_id:Union[int, None]=None, current_time:int=0):
    query = '''
        UPDATE polls
        SET
            "status" = 'inactive',
            ended_at = (?)
        WHERE 1=1
        '''
    query_string, query_data = await extend_filters(query=query, filter_dict={'status': 'active', 'name': name, 'id': poll_id}, limit=None)
    async with sql.connect(path) as con:
        try:
            await con.execute(query_string, (current_time, *query_data))
            await con.commit()
        except sql.OperationalError as err:
            logging.error(f'Failed to deactivate polls {err}')

    logging.info(f'Deactivated poll {name}')


# Get all active polls ordered by most recent
async def get_active_polls(path:str, name:str, limit:int=-1):
    async with sql.connect(path) as con:
        con.row_factory = sql.Row
        query = '''
            WITH sorted_polls AS (
                SELECT *
                FROM polls
                ORDER BY created_at DESC
            )
            SELECT *
            FROM sorted_polls
            WHERE 1=1
            '''
        filter_params = {'status': "active", 'name': name}
        query_string, query_data = await extend_filters(query=query, filter_dict=filter_params, limit=limit)

        active_cursor = await con.execute(
            query_string,
            query_data
        )
        active_data = await active_cursor.fetchall()
    logging.info(f'Retreived active polls with name: {name}')
    return [dict(row) for row in active_data]


# Tally votes for poll
async def tally_votes(path:str, name:str, poll_id:Union[int, None]=None, spacing:int=4):
    async with sql.connect(path) as con:
        con.row_factory = sql.Row
        tally_cursor = await con.execute(
            '''
            WITH selected_polls AS (
                SELECT id
                FROM polls
                WHERE "name" = (?)
                ORDER BY ended_at DESC
                LIMIT 1
            )
            SELECT
                RANK() OVER (ORDER BY COUNT(*) DESC) AS count_rank,
                COUNT(*) AS vote_count, votes.vote_answer
            FROM selected_polls
            LEFT JOIN votes
                ON selected_polls.id = votes.poll_id
            GROUP BY votes.vote_answer, selected_polls.id
            ''',
            (name,)
        )
        tally_data = await tally_cursor.fetchall()

        if not (tally_rows := [dict(row) for row in tally_data]):
            logging.info(f'Poll {name} has no votes')
            tally_cursor = await con.execute(
                '''
                SELECT id, choice_list
                FROM polls
                WHERE "name" = (?)
                ''',
                (name,)
            )
            tally_options = dict(await tally_cursor.fetchone()).get('choice_list')
            tally_rows = [{'count_rank': 1, 'vote_count': 0, 'vote_answer': option} for option in tally_options.split(',')]

    winner_tally = [row for row in tally_rows if row.get('count_rank') == 1]
    col_width = spacing
    result_msg = '\n'.join(
        [
            *[
                '{rank}. {choice: <{width}} {votes}'.format(rank=row['count_rank'], choice=row['vote_answer'], votes=row['vote_count'], width=col_width)
                for row in tally_rows
            ],
            '\n'
            f'''Congratulations to the winner{'s' if len(winner_tally) > 1 else ''} with {winner_tally[0]['vote_count']} total votes:''',
            ', '.join(row['vote_answer'] for row in winner_tally)
        ]
    )
    # TODO: Replace with discord rich embeds
    logging.info(f'Returning result message')
    return result_msg


# Adding additional filter conditions to query
async def extend_filters(query:str, filter_dict:dict, limit:int=-1):
    allowed_filters = ['id', 'name', 'status', 'poll_id']
    query_sections = []
    query_data = []

    for k,v in filter_dict.items():
        if k not in allowed_filters:
            continue

        if isinstance(v, (str, int)):
            query_sections.append(f'AND {k} = (?)')
            query_data.append(v)
        elif isinstance(v, (tuple, list)):
            query_sections.append(f'AND {k} IN (SELECT value FROM json_each(?))')
            query_data.append(json.dumps(v))
        else:
            continue

    if limit:
        query_data.append(limit)
    query_string = '\n'.join([query.strip(), *query_sections, 'LIMIT (?);' if limit != None else ';'])

    logging.info(f'Extended query with fields: {list(filter_dict.keys())}')

    return query_string, query_data