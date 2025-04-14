import azure.functions as func
from omowice_metrics_blueprint import omowice_metrics_blueprint

app = func.FunctionApp()
app.register_functions(omowice_metrics_blueprint)
