import asyncio
import aiopg
import psycopg2
import os

from config import dsn

select = """
SELECT 
    p.slug as pslug, p.content, p.title as ptitle, p.publish_date, c.slug as cslug, c.title as ctitle
FROM blog_blogpost 
AS p
JOIN blog_blogpost_categories
ON blog_blogpost_categories.blogpost_id = p.id
JOIN blog_blogcategory
AS c
ON c.id = blog_blogpost_categories.blogcategory_id
"""

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

@asyncio.coroutine
def go():

    if not os.path.exists('posts'):
        os.makedirs('posts')

    pool = yield from aiopg.create_pool(dsn)
    with (yield from pool.cursor(cursor_factory=psycopg2.extras.DictCursor)) as cur:
        yield from cur.execute(select)
        rows = yield from cur.fetchall()
        curr = None
        for i, row in enumerate(rows):

            print("{} - {} | {}".format(row['publish_date'], row['pslug'], row['ptitle']))
            with open("posts/{}.md".format(row['pslug']), "w") as f:
                f.write(content.format(
                    title=row['ptitle'], 
                    slug=row['pslug'], 
                    date=row['publish_date'],
                    text=row['content']
                ))

loop = asyncio.get_event_loop()
loop.run_until_complete(go())
