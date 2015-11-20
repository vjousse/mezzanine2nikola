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
.. tags: {tags}
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
        size = len(rows)
        curr = None
        tags = []

        for i, row in enumerate(rows):

            tags.append(row['ctitle'])

            if(i+1 < size):
                n = rows[i+1]
                # If next is the same post, don't write the post yet
                # Get all tags first
                if (n['pslug'] == row['pslug']):
                    continue


            #print("{} - {} | {} {}".format(row['publish_date'], row['pslug'], row['ptitle'], row['cslug']))
            #print(tags)

            with open("posts/{}-{}.md".format(str(row['publish_date']).split(" ")[0], row['pslug']), "w") as f:
                f.write(content.format(
                    title=row['ptitle'], 
                    slug=row['pslug'], 
                    date=row['publish_date'],
                    text=row['content'],
                    tags=", ".join(tags)
                ))

            tags = []

loop = asyncio.get_event_loop()
loop.run_until_complete(go())
