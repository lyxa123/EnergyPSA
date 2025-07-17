import sqlite3
import pygame
from datetime import datetime

# Colors (matching pyPSA_db.py style)
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
GREY = (200, 200, 200)
MENU_BG = (50, 50, 50)
MENU_TEXT = (220, 220, 220)
CONFIG_BG = (40, 40, 40)
CONFIG_ITEM_BG = (60, 60, 60)
CONFIG_ITEM_HOVER = (75, 75, 75)
CONFIG_HIGHLIGHT = (0, 200, 0)

# Screen settings
SCREEN_WIDTH = 1050  # Match main application
SCREEN_HEIGHT = 750
# Modify the configuration screen layout constants
CONFIG_SECTION_MARGIN = 40  # Increased from 20
CONFIG_ITEM_HEIGHT = 60    # Increased from 40
CONFIG_ITEM_PADDING = 15   # Increased from 10
CONFIG_ITEM_MARGIN = 20    # Added spacing between items

# Component list panel dimensions
COMPONENT_LIST_WIDTH = 300  # Increased width for component list
COMPONENT_LIST_LEFT = 40    # Left margin
COMPONENT_LIST_TOP = 100    # Top margin after title

# Parameter panel dimensions
PARAM_PANEL_LEFT = COMPONENT_LIST_LEFT + COMPONENT_LIST_WIDTH + 40
PARAM_PANEL_WIDTH = 500

# Button dimensions
BUTTON_HEIGHT = 40
BUTTON_WIDTH = 180
BUTTON_MARGIN = 20

DEFAULT_CONFIGS = {
    "SUPPLIER": {
        "p_nom_mw": {
            "default": 1000.0,
            "min": 100.0,
            "max": 5000.0,
            "description": "Nominal Power Capacity",
            "units": "MW"
        },
        "v_nom_kv": {
            "default": 110.0,
            "min": 11.0,
            "max": 400.0,
            "description": "Nominal Voltage",
            "units": "kV"
        }
    },
    "INDUCTIVE_CONSUMER": {
        "p_demand_rate": {
            "default": 5.0,
            "min": 0.1,
            "max": 100.0,
            "description": "Power Demand",
            "units": "MW"
        },
        "power_factor": {
            "default": 0.8,
            "min": 0.5,
            "max": 0.95,
            "description": "Power Factor",
            "units": "pu"
        }
    },
    "CAPACITIVE_CONSUMER": {
        "p_demand_rate": {
            "default": 5.0,
            "min": 0.1,
            "max": 100.0,
            "description": "Power Demand",
            "units": "MW"
        },
        "power_factor": {
            "default": 0.9,
            "min": 0.85,
            "max": 1.0,
            "description": "Power Factor",
            "units": "pu"
        }
    },
    "RESISTIVE_CONSUMER": {
        "p_demand_rate": {
            "default": 5.0,
            "min": 0.1,
            "max": 100.0,
            "description": "Power Demand",
            "units": "MW"
        },
        "power_factor": {
            "default": 1.0,
            "min": 0.98,
            "max": 1.0,
            "description": "Power Factor",
            "units": "pu"
        }
    }
}

class ConfigurationManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
        self.load_configs()

    def create_tables(self):
        c = self.conn.cursor()
        
        # Component default configurations
        c.execute('''
            CREATE TABLE IF NOT EXISTS ComponentConfigs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                component_type TEXT NOT NULL,
                parameter_name TEXT NOT NULL,
                default_value REAL NOT NULL,
                min_value REAL,
                max_value REAL,
                description TEXT,
                units TEXT,
                UNIQUE(component_type, parameter_name)
            )
        ''')
        
        # User-defined configurations
        c.execute('''
            CREATE TABLE IF NOT EXISTS UserConfigs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                component_type TEXT NOT NULL,
                parameter_name TEXT NOT NULL,
                value REAL NOT NULL,
                last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(component_type, parameter_name)
            )
        ''')
        
        # Populate default configurations if table is empty
        c.execute('SELECT COUNT(*) FROM ComponentConfigs')
        if c.fetchone()[0] == 0:
            for comp_type, params in DEFAULT_CONFIGS.items():
                for param_name, param_data in params.items():
                    c.execute('''
                        INSERT INTO ComponentConfigs 
                        (component_type, parameter_name, default_value, min_value, max_value, description, units)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        comp_type,
                        param_name,
                        param_data['default'],
                        param_data['min'],
                        param_data['max'],
                        param_data['description'],
                        param_data['units']
                    ))
        
        self.conn.commit()

    def load_configs(self):
        """Load configurations from database, falling back to defaults if not found"""
        self.current_configs = {}
        c = self.conn.cursor()
        
        for component_type, params in DEFAULT_CONFIGS.items():
            self.current_configs[component_type] = {}
            for param_name, param_data in params.items():
                # Try to get user config
                c.execute('''
                    SELECT value FROM UserConfigs 
                    WHERE component_type = ? AND parameter_name = ?
                ''', (component_type, param_name))
                result = c.fetchone()
                
                if result:
                    self.current_configs[component_type][param_name] = result[0]
                else:
                    self.current_configs[component_type][param_name] = param_data['default']

    def save_config(self, component_type, parameter_name, value):
        """Save user configuration to database"""
        # Validate value against min/max
        param_data = DEFAULT_CONFIGS[component_type][parameter_name]
        if value < param_data['min'] or value > param_data['max']:
            return False, f"Value must be between {param_data['min']} and {param_data['max']} {param_data['units']}"
        
        c = self.conn.cursor()
        try:
            c.execute('''
                INSERT OR REPLACE INTO UserConfigs (component_type, parameter_name, value)
                VALUES (?, ?, ?)
            ''', (component_type, parameter_name, value))
            self.conn.commit()
            self.load_configs()
            return True, "Configuration saved successfully"
        except Exception as e:
            return False, f"Error saving configuration: {str(e)}"

    def reset_to_default(self, component_type, parameter_name=None):
        """Reset specific or all parameters for a component type to defaults"""
        c = self.conn.cursor()
        if parameter_name:
            c.execute('''
                DELETE FROM UserConfigs 
                WHERE component_type = ? AND parameter_name = ?
            ''', (component_type, parameter_name))
        else:
            c.execute('DELETE FROM UserConfigs WHERE component_type = ?', (component_type,))
        self.conn.commit()
        self.load_configs()

    def reset_all_to_default(self):
        """Reset all configurations to their default values"""
        c = self.conn.cursor()
        c.execute('DELETE FROM UserConfigs')  # Remove all user configurations
        self.conn.commit()
        self.load_configs()
        return "All configurations reset to default values"

class ConfigurationScreen:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.selected_component = None
        self.selected_parameter = None
        self.editing_value = None
        self.message = None
        self.message_timer = 0
        
        # Fonts
        self.title_font = pygame.font.SysFont("Arial", 36)  # Increased from 32
        self.header_font = pygame.font.SysFont("Arial", 28)  # Increased from 24
        self.item_font = pygame.font.SysFont("Arial", 20)   # Increased from 18
        
        # Reset all button
        self.reset_all_button = pygame.Rect(
            SCREEN_WIDTH - 220,  # Position on right side
            20,                  # Near top
            200,                 # Width
            BUTTON_HEIGHT
        )

        # Main menu button
        self.main_menu_button = pygame.Rect(
            20,                  # Left side
            20,                  # Near top
            200,                 # Width
            BUTTON_HEIGHT
        )

    def handle_event(self, event):
        if event.type == pygame.MOUSEBUTTONDOWN:
            # Handle Main Menu button
            if self.main_menu_button.collidepoint(event.pos):
                return "menu"  # Signal to return to main menu
                
            # Handle Reset All button
            if self.reset_all_button.collidepoint(event.pos):
                message = self.config_manager.reset_all_to_default()
                self.message = message
                self.message_timer = pygame.time.get_ticks()
                return True
                
            # Handle component selection
            y = COMPONENT_LIST_TOP
            for component_type in DEFAULT_CONFIGS:
                rect = pygame.Rect(
                    COMPONENT_LIST_LEFT,
                    y,
                    COMPONENT_LIST_WIDTH,
                    CONFIG_ITEM_HEIGHT
                )
                if rect.collidepoint(event.pos):
                    self.selected_component = component_type
                    self.selected_parameter = None
                    return True
                y += CONFIG_ITEM_HEIGHT + CONFIG_ITEM_MARGIN
            
            # Handle parameter selection
            if self.selected_component:
                y = COMPONENT_LIST_TOP
                for param_name in DEFAULT_CONFIGS[self.selected_component]:
                    rect = pygame.Rect(
                        PARAM_PANEL_LEFT,
                        y,
                        PARAM_PANEL_WIDTH,
                        CONFIG_ITEM_HEIGHT
                    )
                    if rect.collidepoint(event.pos):
                        self.selected_parameter = param_name
                        self.editing_value = str(self.config_manager.current_configs[self.selected_component][param_name])
                        return True
                    y += CONFIG_ITEM_HEIGHT + CONFIG_ITEM_MARGIN

        elif event.type == pygame.KEYDOWN and self.editing_value is not None:
            if event.key == pygame.K_RETURN:
                try:
                    value = float(self.editing_value)
                    success, message = self.config_manager.save_config(
                        self.selected_component,
                        self.selected_parameter,
                        value
                    )
                    self.message = message
                    self.message_timer = pygame.time.get_ticks()
                    if success:
                        self.editing_value = str(value)
                except ValueError:
                    self.message = "Invalid number format"
                    self.message_timer = pygame.time.get_ticks()
            elif event.key == pygame.K_BACKSPACE:
                self.editing_value = self.editing_value[:-1]
            elif event.key == pygame.K_ESCAPE:
                self.editing_value = None
            elif event.unicode.isnumeric() or event.unicode == '.':
                self.editing_value += event.unicode
        
        return False

    def draw(self, surface):
        # Fill background
        surface.fill(CONFIG_BG)
        
        # Draw title
        title = self.title_font.render("Component Configuration", True, WHITE)
        surface.blit(title, (SCREEN_WIDTH // 2 - title.get_width() // 2, 30))
        
        # Draw Main Menu button
        pygame.draw.rect(surface, (0, 100, 150), self.main_menu_button)  # Blue color
        menu_text = self.header_font.render("Main Menu", True, WHITE)
        text_rect = menu_text.get_rect(center=self.main_menu_button.center)
        surface.blit(menu_text, text_rect)
        
        # Draw Reset All button
        pygame.draw.rect(surface, (150, 0, 0), self.reset_all_button)  # Dark red color
        reset_all_text = self.header_font.render("Reset All", True, WHITE)
        text_rect = reset_all_text.get_rect(center=self.reset_all_button.center)
        surface.blit(reset_all_text, text_rect)
        
        # Draw component list
        y = COMPONENT_LIST_TOP
        for component_type in DEFAULT_CONFIGS:
            bg_color = CONFIG_HIGHLIGHT if component_type == self.selected_component else CONFIG_ITEM_BG
            rect = pygame.Rect(COMPONENT_LIST_LEFT, y, COMPONENT_LIST_WIDTH, CONFIG_ITEM_HEIGHT)
            pygame.draw.rect(surface, bg_color, rect, border_radius=5)
            
            text = self.item_font.render(component_type.replace("_", " "), True, WHITE)
            text_rect = text.get_rect(center=rect.center)
            surface.blit(text, text_rect)
            
            y += CONFIG_ITEM_HEIGHT + CONFIG_ITEM_MARGIN
        
        # Draw parameters if component selected
        if self.selected_component:
            y = COMPONENT_LIST_TOP
            for param_name in DEFAULT_CONFIGS[self.selected_component]:
                param_data = DEFAULT_CONFIGS[self.selected_component][param_name]
                bg_color = CONFIG_HIGHLIGHT if param_name == self.selected_parameter else CONFIG_ITEM_BG
                rect = pygame.Rect(PARAM_PANEL_LEFT, y, PARAM_PANEL_WIDTH, CONFIG_ITEM_HEIGHT)
                pygame.draw.rect(surface, bg_color, rect, border_radius=5)
                
                # Parameter name and description
                name_text = self.item_font.render(param_data['description'], True, WHITE)
                surface.blit(name_text, (rect.x + 20, rect.y + 10))
                
                # Current value
                if param_name == self.selected_parameter and self.editing_value is not None:
                    value_text = self.item_font.render(f"{self.editing_value} {param_data['units']}", True, WHITE)
                else:
                    current_value = self.config_manager.current_configs[self.selected_component][param_name]
                    value_text = self.item_font.render(f"{current_value} {param_data['units']}", True, WHITE)
                surface.blit(value_text, (rect.x + 20, rect.y + rect.height - 30))
                
                y += CONFIG_ITEM_HEIGHT + CONFIG_ITEM_MARGIN
        
        # Draw message if exists
        if self.message and pygame.time.get_ticks() - self.message_timer < 3000:
            msg_text = self.item_font.render(self.message, True, WHITE)
            msg_rect = msg_text.get_rect()
            msg_rect.centerx = SCREEN_WIDTH // 2
            msg_rect.bottom = SCREEN_HEIGHT - 20
            
            # Draw message background
            bg_rect = msg_rect.inflate(40, 20)
            pygame.draw.rect(surface, (0, 0, 0), bg_rect)
            
            surface.blit(msg_text, msg_rect) 