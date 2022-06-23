from flask_restplus import reqparse

params = reqparse.RequestParser()

params.add_argument('qtr', type=str, required = False, help = 'Quarter filter in YYYY QQ format')
params.add_argument('busunit', type=str, required = False, help = 'Business Unit filter')
params.add_argument('process', type=str, required = False, help = 'Process filter')
params.add_argument('geo', type=str, required = False, help = 'Geo filter')
params.add_argument('market', type=str, required = False, help = 'Market filter')
#params.add_argument('country', type=str, required = False, help = 'Country filter')

params_ctrl = reqparse.RequestParser()

params_ctrl.add_argument('qtr', type=str, required = False, help = 'Quarter filter in YYYY QQ format')
params_ctrl.add_argument('busunit', type=str, required = False, help = 'Business Unit filter')
params_ctrl.add_argument('process', type=str, required = False, help = 'Process filter')
params_ctrl.add_argument('geo', type=str, required = False, help = 'Geo filter')
params_ctrl.add_argument('market', type=str, required = False, help = 'Market filter')
#params_ctrl.add_argument('country', type=str, required = False, help = 'Country filter')
params_ctrl.add_argument('func_exec_ctrl', type=str, required = False, help = 'FUNCTION EXCUTG CTRL filter')

params_audit = reqparse.RequestParser()

params_audit.add_argument('qtr', type=str, required = False, help = 'Quarter filter in QQYYYY format')
params_audit.add_argument('engmt', type=str, required = False, help = 'Engagement filter')
params_audit.add_argument('geo', type=str, required = False, help = 'Geo filter')
params_audit.add_argument('mkt', type=str, required = False, help = 'Market filter')