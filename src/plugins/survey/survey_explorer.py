# -*- coding:utf-8 -*-

#
# This file is part of OpenFisca.
# OpenFisca is a socio-fiscal microsimulation software
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul
# Licensed under the terms of the GVPLv3 or later license
# (see openfisca/__init__.py for details)


from os import path
from pandas import DataFrame

from src.gui.qt.QtGui import (QWidget, QLabel, QVBoxLayout, QHBoxLayout, QPushButton,
                         QSpacerItem, QSizePolicy, QInputDialog, 
                         QGroupBox, QButtonGroup, QDockWidget)
from src.gui.qt.QtCore import SIGNAL, Signal,Qt
from src.gui.qt.compat import to_qvariant

from src.gui.utils.qthelpers import create_action
from src.gui.qthelpers import OfSs, DataFrameViewWidget, MyComboBox

from src.lib.columns import EnumCol

from src.gui.config import CONF, get_icon
from src.plugins.__init__ import OpenfiscaPluginWidget, PluginConfigPage
from src.gui.baseconfig import get_translation
_ = get_translation('src')

from datetime import datetime
from src.gui.qt.compat import from_qvariant


class SurveyExplorerConfigPage(PluginConfigPage):
    def __init__(self, plugin, parent):
        PluginConfigPage.__init__(self, plugin, parent)
        self.get_name = lambda: _("Survey data explorer")
        
    def setup_page(self):
        '''
        Setup the page of the survey widget
        '''
        
        survey_group = QGroupBox(_("Survey data")) # "Données d'enquête" 
        survey_bg = QButtonGroup(self)
        survey_label = QLabel(_("Location of survey data for microsimulation")) # u"Emplacement des données d'enquête pour la microsimulation")

        bareme_only_radio = self.create_radiobutton(_("Test case only"),  #u"Mode barème uniquement",
                                                    'bareme_only', False,
                                                    tip = u"Mode barème uniquement",
                                                    
                                button_group = survey_bg)
        survey_radio = self.create_radiobutton(_("The following file"),  # le fichier suivant",
                                               'enable', True,
                                               _("Survey data file for micrsosimulation"), # "Fichier de données pour la microsimulation",
                                               button_group=survey_bg)
        survey_file = self.create_browsefile("", 'data_file',
                                             filters='*.h5')
        
        self.connect(bareme_only_radio, SIGNAL("toggled(bool)"),
                     survey_file.setDisabled)
        self.connect(survey_radio, SIGNAL("toggled(bool)"),
                     survey_file.setEnabled)
        survey_file_layout = QHBoxLayout()
        survey_file_layout.addWidget(survey_radio)
        survey_file_layout.addWidget(survey_file)

        survey_layout = QVBoxLayout()
        survey_layout.addWidget(survey_label)
        survey_layout.addWidget(bareme_only_radio)
        survey_layout.addLayout(survey_file_layout)
        survey_group.setLayout(survey_layout)

#        reform_group = QGroupBox(_("Reform"))
#        reform = self.create_checkbox(_('Reform mode'), 'reform')
#                 
#        layout = QVBoxLayout()
#        layout.addWidget(reform)
#        reform_group.setLayout(layout)

        
        vlayout = QVBoxLayout()
        vlayout.addWidget(survey_group)
#        vlayout.addWidget(reform_group)
        vlayout.addStretch(1)
        self.setLayout(vlayout)


class SurveyExplorerWidget(OpenfiscaPluginWidget):    
    """
    Survey data explorer Widget
    """
    CONF_SECTION = 'survey'
    CONFIGWIDGET_CLASS = SurveyExplorerConfigPage
    LOCATION = Qt.LeftDockWidgetArea
    FEATURES = QDockWidget.DockWidgetClosable | \
               QDockWidget.DockWidgetFloatable | \
               QDockWidget.DockWidgetMovable
    DISABLE_ACTIONS_WHEN_HIDDEN = False
    sig_option_changed = Signal(str, object)

    def __init__(self, parent = None):
        super(SurveyExplorerWidget, self).__init__(parent)
        self.setStyleSheet(OfSs.dock_style)
        # Create geometry
        self.setObjectName( _("Survey data explorer"))
        self.dockWidgetContents = QWidget()
        
        self.data_label = QLabel("Data", self.dockWidgetContents)

        self.add_btn = QPushButton(_("Add a variable"), self.dockWidgetContents)  # "Ajouter variable"      
        self.remove_btn = QPushButton(_("Remove a variable"), self.dockWidgetContents) # Retirer une variable 
        self.datatables_choices = []
        self.datatable_combo = MyComboBox(self.dockWidgetContents, _("Choose a table"), self.datatables_choices) # u'Choix de la table'
        
        
        spacerItem = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        horizontalLayout = QHBoxLayout()
        horizontalLayout.addWidget(self.add_btn)
        horizontalLayout.addWidget(self.remove_btn)
        horizontalLayout.addWidget(self.datatable_combo)
        horizontalLayout.addItem(spacerItem)
        self.view = DataFrameViewWidget(self.dockWidgetContents)

        verticalLayout = QVBoxLayout(self.dockWidgetContents)
        verticalLayout.addWidget(self.data_label)

        verticalLayout.addLayout(horizontalLayout)
        verticalLayout.addWidget(self.view)
        self.setLayout(verticalLayout)

        # Initialize attributes
        self.parent = parent

        self.selected_vars = set()
        self.data = DataFrame() 
        self.view_data = None
        self.dataframes = {}
        self.vars = set()

        self.initialize()
        self.connect(self.add_btn, SIGNAL('clicked()'), self.add_var)
        self.connect(self.remove_btn, SIGNAL('clicked()'), self.remove_var)
        self.connect(self.datatable_combo.box, SIGNAL('currentIndexChanged(int)'), self.select_data)        
        self.update_btns()
        self.initialize_plugin() # To run the suitable inherited API methods
        
    def initialize(self):
        """
        Initialize widget
        """
        # Set survey_simulation
        
        if (not self.get_option('enable')) or (self.main.survey_simulation is None):
            return
                
        simulation = self.main.survey_simulation
        country = CONF.get('parameters', 'country')
        value = CONF.get('parameters', 'datesim')
        datesim = datetime.strptime(value ,"%Y-%m-%d").date()
        reforme = self.get_option('reform')
        year = datesim.year
        simulation.set_config(year = year, country = country, reforme = reforme)
        self.simulation = simulation

    def load_data(self):
        simulation = self.main.survey_simulation
        # load_from_file(self):        
        fname = self.get_option('data_file')
        if path.isfile(fname):
            try:
                simulation.set_survey(filename = fname)
                year = simulation.survey.survey_year
                # Sets year in label
                self.data_label.setText("Survey data from year " + str(year))
                self.add_dataframe(simulation.survey.table, name = "input")
                self.set_dataframe(simulation.survey.table, name = "input")
                self.update_view()
            except:
                print 'Survey data loading failed: disabling survey mode'
                self.set_option('bareme_only', True)
                 
#                raise Exception('Survey data loading failed')
                
        self.simulation = simulation
            
    def update_btns(self):
        if (self.vars - self.selected_vars):
            self.add_btn.setEnabled(True)
        if self.selected_vars:
            self.remove_btn.setEnabled(True)
            
    def update_choices(self):
        box = self.datatable_combo.box
        box.clear()
        for name, key in self.datatables_choices:
            box.addItem(name, to_qvariant(key))

    def select_data(self):
        widget = self.datatable_combo.box
        dataframe_name = from_qvariant(widget.itemData(widget.currentIndex()))
        try: # to deal with the box.clear() 
            self.set_dataframe(name = dataframe_name)
        except:
            pass

        self.update_btns()

    def add_dataframe(self, dataframe, name = None):
        '''
        Adds a dataframe to the list o the available dataframe 
        '''
        if name == None:
            name = "dataframe" + len(self.dataframes.keys())

        self.dataframes[name] = dataframe
        self.datatables_choices.append((name, name))
        self.update_choices()
        self.update_btns()

    def set_dataframe(self, dataframe = None, name = None):
        '''
        Sets the current dataframe
        '''
        if name is not None:
            self.data = self.dataframes[name]
        if dataframe is not None:
            self.data = dataframe
            
        self.vars = set(self.data.columns)
        self.update_btns()

    def ask(self, remove=False):
        if not remove:
            label = "Ajouter une variable"
            choices = self.vars - self.selected_vars
        else:
            choices =  self.selected_vars
            label = "Retirer une variable"
            
        var, ok = QInputDialog.getItem(self, label , "Choisir la variable", 
                                       sorted(list(choices)))
        if ok and var in list(choices): 
            return str(var)
        else:
            return None 

    def add_var(self):
        var = self.ask()
        if var is not None:
            self.selected_vars.add(var)
            self.update_view()
        else:
            return
    
    def remove_var(self):
        var = self.ask(remove=True)
        if var is not None:
            self.selected_vars.remove(var)
            self.update_view()
        else:
            return
                         
    def set_choices(self, description):
        '''
        Set the variables appearing in the add and remove dialogs
        
        Parameters
        ----------
        
        description : TODO: 
        '''     
        data_vars = set(self.data.columns)
        label2var = {}
        var2label = {}
        var2enum = {}
        for var in description.col_names:
            varcol  = description.get_col(var)
            if isinstance(varcol, EnumCol): 
                var2enum[var] = varcol.enum
                if varcol.label:
                    label2var[varcol.label] = var
                    var2label[var]          = varcol.label
                else:
                    label2var[var] = var
                    var2label[var] = var
                    
        self.var2label = var2label
        self.var2enum  = var2enum
        
    def update_view(self):
        self.starting_long_process(_("Updating survey explorer table"))
        if not self.selected_vars:
            self.view.clear()
            self.ending_long_process(_("Survey explorer table updated"))    
            return
        
        cols = self.selected_vars
        if self.view_data is None:
            self.view_data = self.data[list(cols)]
        
        new_col = cols - set(self.view_data)
        if new_col:
            self.view_data[list(new_col)] = self.data[list(new_col)] 
            df = self.view_data
        else:
            df = self.view_data[list(cols)]
    
        self.view.set_dataframe(df)
        self.view.reset()
        self.update_btns()
        self.ending_long_process(_("Survey explorer table updated"))
        
    def calculated(self):
        self.emit(SIGNAL('calculated()'))
                
    def clear(self):
        """
        Clear view and dataframes
        """
        self.view.clear()
        self.data = None
        self.datatables_choices = []
        self.dataframes = {}
        self.update_btns()
        
        
    def compute(self):
        '''
        Compute survey_simulation
        '''
        self.starting_long_process(_("Ongoing microsimulation ..."))
        P, P_default = self.main.parameters.getParam(), self.main.parameters.getParam(defaut = True)
        self.simulation.set_param(P, P_default)
        self.simulation.compute()
        self.action_compute.setEnabled(False)
        self.ending_long_process(_("Microsimulation results are updated"))
        self.main.refresh_survey_plugins()

    def set_reform(self, reform):
        """
        Set reform mode
        """
        self.simulation.set_config(reforme = reform)
        self.set_option('reform', reform)
        self.action_compute.setEnabled(True)
         
    #------ OpenfiscaPluginMixin API ---------------------------------------------
    
    def apply_plugin_settings(self, options):
        """
        Apply configuration file's plugin settings
        """
        if 'reform' in options:
            self.action_set_reform.setChecked(self.get_option('reform'))
            
        if 'bareme_only' in options:
            self.main.register_survey_widgets(self.main.scenario_simulation.country)
            
        if 'data_file' in options:
            self.initialize()
    
    #------ OpenfiscaPluginWidget API ---------------------------------------------

    def get_plugin_title(self):
        """
        Return plugin title
        Note: after some thinking, it appears that using a method
        is more flexible here than using a class attribute
        """
        return _("Survey Data Explorer")

    
    def get_plugin_icon(self):
        """
        Return plugin icon (QIcon instance)
        Note: this is required for plugins creating a main window
              (see OpenfiscaPluginMixin.create_mainwindow)
              and for configuration dialog widgets creation
        """
        return get_icon('OpenFisca22.png')
            
    def get_plugin_actions(self):
        """
        Return a list of actions related to plugin
        Note: these actions will be enabled when plugin's dockwidget is visible
              and they will be disabled when it's hidden
        """
#        self.open_action = create_action(self, _("&Open..."),
#                icon='fileopen.png', tip=_("Open survey data file"),
#                triggered=self.load)
#        self.register_shortcut(self.open_action, context="Survey data explorer",
#                               name=_("Open survey data file"), default="Ctrl+O")
#        self.save_action = create_action(self, _("&Save"),
#                icon='filesave.png', tip=_("Save current microsimulation data"),
#                triggered=self.save)
#
#        self.register_shortcut(self.save_action, context="Survey data explorer",
#                               name=_("Save current data microsimulation"), default="Ctrl+S")
#
#        self.file_menu_actions = [self.open_action, self.save_action,]
#        self.main.file_menu_actions += self.file_menu_actions
        self.action_compute = create_action(self, _('Compute aggregates'),
                                                      shortcut = 'F10',
                                                      icon = 'calculator_blue.png', 
                                                      triggered = self.compute)
        self.register_shortcut(self.action_compute, 
                               context = 'Survey explorer',
                                name = _('Compute survey simulation'), default = 'F10')

        self.action_set_reform = create_action(self, _('Reform mode'), 
                                                     icon = 'comparison22.png', 
                                                     toggled = self.set_reform, 
                                                     tip = u"Différence entre la situation simulée et la situation actuelle")

        self.run_menu_actions = [self.action_compute, self.action_set_reform]
        self.main.run_menu_actions += self.run_menu_actions        
        self.main.survey_toolbar_actions += self.run_menu_actions 
        
#        return self.file_menu_actions + self.run_menu_actions
        return self.run_menu_actions

    
    def register_plugin(self):
        """
        Register plugin in OpenFisca's main window
        """
        self.main.add_dockwidget(self)

    def refresh_plugin(self):
        '''
        Update Survey dataframes
        '''
        self.starting_long_process(_("Refreshing survey explorer dataframe ..."))
        simulation = self.main.survey_simulation        
        if simulation.outputs is not None:
            self.add_dataframe(simulation.outputs.table, 
                               name = "output")
        if simulation.reforme:
            if simulation.outputs_default is not None:
                self.add_dataframe(simulation.outputs_default.table, 
                                   name = "output_default")
        self.update_choices()
        self.update_btns()
        self.update_view()
        self.ending_long_process(_("Survey explorer dataframe updated"))
    
    def closing_plugin(self, cancelable=False):
        """
        Perform actions before parent main window is closed
        Return True or False whether the plugin may be closed immediately or not
        Note: returned value is ignored if *cancelable* is False
        """
        return True

        