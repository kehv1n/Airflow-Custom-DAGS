# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_admin import BaseView, expose


class Longitudinal(BaseView):
    @expose('/')
    def main(self):
        # Connect to Database (mySQL) that is being run locally (VM).

        # JSON Query's





        return self.render("mm_data_over_time/trends.html",
                           content="Hello Longitudinal Quality Statistics!")








v = Longitudinal(category="Trends", name="Longitudinal Vizualization")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "data_over_time", __name__,
# registers airflow/plugins/templates as a Jinja template folder
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/test_plugin')


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    admin_views = [v]
    flask_blueprints = [bp]
    # menu_links = [ml]
