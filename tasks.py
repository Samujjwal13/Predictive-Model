"""this is example file used for code coverage report"""
import path
import invoke
def get_pylint_args():
    """this is function to get all the path of all files"""
    top_path = path.Path(".")
    top_dirs = top_path.dirs()
    for top_dir in top_dirs:
        if top_dir.joinpath("__init__.py").exists():
            yield top_dir
    yield from (x for x in top_path.files("*.py"))
@invoke.task
def build(pylint, docs=False):
    """this function invokes all pylint shells """
    pylint.run("pylint " + " ".join(get_pylint_args()), echo=True)
