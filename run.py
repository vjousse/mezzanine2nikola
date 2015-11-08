import asyncio
import aiopg
import psycopg2

from config import dsn

@asyncio.coroutine
def go():
    pool = yield from aiopg.create_pool(dsn)
    with (yield from pool.cursor(cursor_factory=psycopg2.extras.DictCursor)) as cur:
        yield from cur.execute("SELECT slug, content, title, publish_date FROM blog_blogpost")
        rows = yield from cur.fetchall()
        for row in rows:
            print("{} - {} | {}".format(row['publish_date'], row['slug'], row['title']))
            with open("{}.md".format(row['slug']), "w") as f:
                content = """<!-- 
.. title: {title}
.. slug: {slug}
.. date: {date}
.. tags:
.. category: 
.. link: 
.. description: 
.. type: text
-->

{text}
"""
                f.write(content.format(
                    title=row['title'], 
                    slug=row['slug'], 
                    date=row['publish_date'],
                    text=row['content']
                ))

loop = asyncio.get_event_loop()
loop.run_until_complete(go())
