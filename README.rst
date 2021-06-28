binancebot
==========

Running the bot
---------------

This project uses poetry_ for dependency and environment management.

Copy the config template to e.g. config.toml and put your API keys in it.

Then run the bot with ::

   poetry install
   poetry run python -m binancebot --config-file config.toml

Run the tests (such as they are) with ::

   poetry run pytest


Development
-----------

I use asyncio and httpx to parallelize requests against the binance API,
so it'd be worth going over a tutorial for python async programming before making contributions.
As far as I'm aware our use of the libraries is very straightforward.
I also use mypy and extensive type annotations, which again it would be worth becoming familiar with.
I use pre-commit_ to maintain code standards, and if you'd like to contribute you should too.
It'll autoformat your code and pick you up on trivial errors like missing imports
and less trivial errors like type mismatches.

The project layout is fairly straightforwrd:

-  ``binancebot.trader`` defines an interface for a generic trading client,
   and contains the logic for making trades against it to keep a balanced portfolio.
   I've tried to keep this as generic as possible (mostly for clarity of thought),
   but some binance-specific stuff might have leaked anyway. I'm lazy.
-  ``binancebot.binance`` defines a client for the binance API that satisfies the interface.
-  ``binancebot.__main__`` loads the config, launches the client, sets up logging, etc.
   It also has a little bit of trading logic
   (setting the target distribution based on recent price movements)
   because I'm lazy.
-  ``binancebot.server`` defines a simple HTTP server that gives debug information about the bot.
   At some point I'll extend this to give yet more useful information, and maybe even show some pretty graphs.

.. _poetry: https://python-poetry.org/
.. _pre-commit: https://pre-commit.com/
