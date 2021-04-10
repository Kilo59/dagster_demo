import invoke


@invoke.task
def sort(ctx):
    ctx.run("isort --profile black .")


@invoke.task(pre=[sort])
def fmt(ctx):
    ctx.run("black .")
