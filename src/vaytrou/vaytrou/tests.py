import shutil
import tempfile

import vaytrou.admin

# Admin tests

def test_admin():
    admin = vaytrou.admin.IndexAdmin()
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

def test_batch():
    data = tempfile.mkdtemp()
    admin = vaytrou.admin.IndexAdmin()
    admin.run(['-d', data, 'create', 'foo'])
    admin.run(['-d', data, 'batch', 'foo', '-f', 'index-st99_d00.json'])
    admin.run(['-d', data, 'dump', 'foo'])
    shutil.rmtree(data)

def test_pack():
    data = tempfile.mkdtemp()
    admin = vaytrou.admin.IndexAdmin()
    admin.run(['-d', data, 'create', 'foo'])
    admin.run(['-d', data, 'pack', 'foo'])
    shutil.rmtree(data)

