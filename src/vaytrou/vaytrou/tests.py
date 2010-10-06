import vaytrou.admin

# Admin tests

def test_admin():
    admin = vaytrou.admin.IndexAdmin()

# Command tests

def test_base_command():
    cmd = vaytrou.admin.BaseCommand()
    cmd.help()

def test_batch_command():
    cmd = vaytrou.admin.BatchCommand()
    cmd.help()
