import shutil
import tempfile

import vaytrou.admin

# Admin tests

def test_admin():
    storage = tempfile.mkdtemp()
    admin = vaytrou.admin.IndexAdmin()
    index = admin.find_index(storage)
    assert hasattr(index, 'fwd')
    shutil.rmtree(storage)
    def foo(*args):
        pass
    admin.foo = foo
    admin.run(['foo', 'bar', 'x'])
    admin.run(['help', 'info'])
    admin.run(['info', '--help'])

def test_ro_commands():
    data = tempfile.mkdtemp()
    admin = vaytrou.admin.IndexAdmin()
    admin.run(['-d', data, 'create', 'foo'])
    admin.run(['-d', data, 'info', 'foo'])
    admin.run(['-d', data, 'dump', 'foo'])
    admin.run(['-d', data, 'search', 'foo', '--', '0,0,0,0'])
    shutil.rmtree(data)

# Command tests

def test_base_command():
    cmd = vaytrou.admin.BaseCommand()
    cmd.help()

def test_batch_command():
    cmd = vaytrou.admin.BatchCommand()
    cmd.help()
