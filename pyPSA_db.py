import pygame
import pypsa
import pandas as pd
import numpy as np
import math
import collections
from datetime import datetime
import sys

# Add imports at the top
from config_manager import ConfigurationManager, ConfigurationScreen

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
GREEN = (0, 200, 0)
RED = (200, 0, 0)
BLUE = (0, 0, 255)
GREY = (200, 200, 200)
YELLOW = (255, 255, 0)
LOG_PANEL_BG = (30, 30, 30)
LOG_TEXT_COLOR = (220, 220, 220)
ENTITY_LABEL_COLOR = BLACK
MENU_BG = (50, 50, 50)
MENU_TEXT = (220, 220, 220)
HELP_BG = (40, 40, 40)

# Component Types
COMPONENT_TYPES = {
    "SUPPLIER": {
        "label": "S",
        "description": "Power Supplier",
        "icon_color": GREEN
    },
    "INDUCTIVE_CONSUMER": {
        "label": "CI",
        "description": "Inductive Load",
        "icon_color": BLUE
    },
    "CAPACITIVE_CONSUMER": {
        "label": "CC",
        "description": "Capacitive Load",
        "icon_color": BLUE
    },
    "RESISTIVE_CONSUMER": {
        "label": "CR",
        "description": "Resistive Load",
        "icon_color": BLUE
    }
}

# Sidebar Settings from pyPSA_3D.py
SIDEBAR_WIDTH = 250
MAIN_AREA_WIDTH = 800
SCREEN_WIDTH = MAIN_AREA_WIDTH + SIDEBAR_WIDTH  # Update screen width to include sidebar
SCREEN_HEIGHT = 750
FPS = 60
SIDEBAR_ANIMATION_SPEED = 15
SIDEBAR_SHADOW_WIDTH = 10
SIDEBAR_SHADOW_ALPHA = 128
SIDEBAR_ITEM_HEIGHT = 80
SIDEBAR_ITEM_PADDING = 20
SIDEBAR_ITEM_MARGIN = 10
SIDEBAR_TITLE_HEIGHT = 50
SIDEBAR_ITEM_WIDTH = SIDEBAR_WIDTH - (2 * SIDEBAR_ITEM_PADDING)
SIDEBAR_BG = (45, 45, 45)
SIDEBAR_ITEM_BG = (60, 60, 60)
SIDEBAR_ITEM_HOVER = (75, 75, 75)
SIDEBAR_TEXT = (220, 220, 220)
SIDEBAR_SHADOW_COLOR = (0, 0, 0)
SIDEBAR_BUTTON_TEXT = "⚙"

# --- PyGame Settings ---
# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
GREEN = (0, 200, 0)
RED = (200, 0, 0)
BLUE = (0, 0, 255)
GREY = (200, 200, 200)
YELLOW = (255, 255, 0)
LOG_PANEL_BG = (30, 30, 30)
LOG_TEXT_COLOR = (220, 220, 220)
ENTITY_LABEL_COLOR = BLACK
MENU_BG = (50, 50, 50)
MENU_TEXT = (220, 220, 220)
HELP_BG = (40, 40, 40)

# Menu Settings
MENU_TITLE_SIZE = 48
MENU_ITEM_SIZE = 36
MENU_START_Y = SCREEN_HEIGHT // 3
MENU_SPACING = 80
MENU_BUTTON_WIDTH = 300
MENU_BUTTON_HEIGHT = 60
MENU_BUTTON_PADDING = 20
MENU_TITLE_COLOR = WHITE
MENU_ITEM_COLOR = (200, 200, 200)
MENU_SELECTED_COLOR = GREEN

# Button Settings
BUTTON_SIZE = 30
BUTTON_PADDING = 10
BUTTON_COLOR = (100, 100, 100)
BUTTON_HOVER_COLOR = (150, 150, 150)
BUTTON_TEXT_COLOR = WHITE

# Entity Settings
SUPPLIER_SIZE = 60
CONSUMER_SIZE = 45
CONNECTION_COLOR = GREY
CONNECTION_THICKNESS = 3
ENTITY_FONT_SIZE = 16

# --- PyPSA Settings ---
SIM_STEP_INTERVAL = 1
DEFAULT_SUPPLIER_P_GENERATION = 20
DEFAULT_SUPPLIER_Q_CAPABILITY = 15
DEFAULT_SUPPLIER_P_CAPACITY = 1000
SUPPLIER_LOW_THRESHOLD_PERCENT = 0.2

# Consumer Settings
DEFAULT_CONSUMER_P_DEMAND = 5
CONSUMER_TYPE_INDUCTIVE = "inductive"
CONSUMER_TYPE_CAPACITIVE = "capacitive"
CONSUMER_TYPE_RESISTIVE = "resistive"

DEFAULT_INDUCTIVE_PF = 0.8
DEFAULT_CAPACITIVE_PF = 0.9
DEFAULT_RESISTIVE_PF = 1.0

# --- Menu Settings ---
MENU_HEIGHT = 40
MENU_RECT = pygame.Rect(0, 0, SCREEN_WIDTH, MENU_HEIGHT)
HELP_BUTTON_RECT = pygame.Rect(SCREEN_WIDTH - 100, 5, 90, 30)
HELP_TEXT = """
Energy City Simulator Help:

Controls:
- Left Click on Grid: Select placement area
- Click Component: Place selected component at highlighted area
- Shift + Left Click: Select consumer to connect
- Left Click on Supplier: Complete connection
- Left Click Empty Space: Cancel connection
- ESC: Cancel selection or connection

Colors:
- Green: Normal operation
- Yellow: Warning (voltage/loading)
- Red: Critical (low voltage/overload)
- Blue: Unconnected consumer
- White: Selected placement area

Status:
- Check log panel for power flow info
- Voltage levels shown in real-time
- Connection status displayed
"""

# --- Log Panel Settings ---
LOG_PANEL_HEIGHT = 150
LOG_PANEL_RECT = pygame.Rect(0, SCREEN_HEIGHT - LOG_PANEL_HEIGHT, SCREEN_WIDTH, LOG_PANEL_HEIGHT)
MAX_LOG_MESSAGES = 8

# --- 3D Settings ---
CUBE_SIZE = 64
TILE_WIDTH = CUBE_SIZE
TILE_HEIGHT = int(CUBE_SIZE * math.sin(math.radians(30)))
TILE_DEPTH = CUBE_SIZE
CONNECTION_OFFSET = 16

# --- Global Variables ---
network = None
pygame_entities = []
supplier_entity = None
show_help = False
connecting_consumer = None
temp_connection_line_end = None
log_messages = collections.deque(maxlen=MAX_LOG_MESSAGES)
log_font = None
entity_font = None
menu_font = None
selected_grid_pos = None  # Store the currently selected grid position
selected_component_type = None  # Store the currently selected component type

# Add after the global variables section
config_manager = None

# Color adjustments for 3D
def darken_color(color, factor=0.7):
    return tuple(int(c * factor) for c in color)

def lighten_color(color, factor=1.3):
    return tuple(min(255, int(c * factor)) for c in color)

def to_isometric(grid_x, grid_y):
    """Convert grid coordinates to isometric screen coordinates"""
    iso_x = (grid_x - grid_y) * TILE_WIDTH
    iso_y = (grid_x + grid_y) * TILE_HEIGHT
    return iso_x, iso_y

class Button:
    def __init__(self, x, y, size, text, color):
        self.rect = pygame.Rect(x, y, size, size)
        self.text = text
        self.color = color
        self.hover_color = BUTTON_HOVER_COLOR
        self.is_hovered = False
        
    def draw(self, surface):
        color = self.hover_color if self.is_hovered else self.color
        pygame.draw.rect(surface, color, self.rect)
        if self.text:
            font = pygame.font.SysFont("Arial", BUTTON_SIZE - 10)
            text_surface = font.render(self.text, True, BUTTON_TEXT_COLOR)
            text_rect = text_surface.get_rect(center=self.rect.center)
            surface.blit(text_surface, text_rect)
    
    def handle_event(self, event):
        if event.type == pygame.MOUSEMOTION:
            self.is_hovered = self.rect.collidepoint(event.pos)
        elif event.type == pygame.MOUSEBUTTONDOWN:
            if event.button == 1 and self.rect.collidepoint(event.pos):
                return True
        return False

def add_log_message(message):
    max_len = SCREEN_WIDTH // (log_font.size("A")[0] if log_font and log_font.size("A")[0] > 0 else 8) - 3
    log_messages.append(message[:max_len] + "..." if len(message) > max_len else message)

def draw_outlined_text(surface, text, font, x, y, text_color, outline_color=BLACK):
    # Draw outline
    for dx in [-1, 0, 1]:
        for dy in [-1, 0, 1]:
            if dx != 0 or dy != 0:
                outline_surface = font.render(text, True, outline_color)
                surface.blit(outline_surface, (x + dx, y + dy))
    # Draw main text
    text_surface = font.render(text, True, text_color)
    surface.blit(text_surface, (x, y))

class Entity(pygame.sprite.Sprite):
    def __init__(self, x, y, size, color, label_text, bus_name=None, entity_type="unknown"):
        super().__init__()
        # Store grid position directly
        self.grid_x = x
        self.grid_y = y
        
        self.base_color = color
        self.label_text = label_text
        self.bus_name = bus_name
        self.entity_type = entity_type
        self.dragging = False
        self.connected_to = None
        self.is_connected = False
        self.voltage_pu = 1.0
        self.total_p_demand = 0
        self.total_q_demand = 0
        self.p_demand_rate = 0
        self.power_factor = 1.0
        
        # Create surface for the entity - match selection highlight size
        self.image = pygame.Surface((TILE_WIDTH * 2, TILE_WIDTH * 2), pygame.SRCALPHA)
        self.rect = self.image.get_rect()
        self.redraw()

    def redraw(self):
        self.image.fill((0, 0, 0, 0))  # Transparent background
        
        # Calculate colors for different faces
        top_color = self.base_color
        right_color = darken_color(self.base_color, 0.8)  # Slightly darker
        left_color = darken_color(self.base_color, 0.6)   # Darkest
        
        # Calculate points for isometric cube
        center_x = self.image.get_width() // 2
        center_y = self.image.get_height() // 2
        
        # Use exact same dimensions as selection highlight
        half_width = TILE_WIDTH // 2
        half_height = TILE_HEIGHT
        
        # Define cube points to match isometric grid
        top_face = [
            (center_x, center_y - half_height),  # Top
            (center_x + half_width, center_y - half_height//2),  # Right
            (center_x, center_y),  # Bottom
            (center_x - half_width, center_y - half_height//2)   # Left
        ]
        
        left_face = [
            (center_x - half_width, center_y - half_height//2),  # Top
            (center_x, center_y),  # Right
            (center_x, center_y + half_height//2),  # Bottom
            (center_x - half_width, center_y)   # Left
        ]
        
        right_face = [
            (center_x + half_width, center_y - half_height//2),  # Top
            (center_x + half_width, center_y),  # Right
            (center_x, center_y + half_height//2),  # Bottom
            (center_x, center_y)  # Left
        ]

        # Draw faces
        pygame.draw.polygon(self.image, top_color, top_face)
        pygame.draw.polygon(self.image, left_color, left_face)
        pygame.draw.polygon(self.image, right_color, right_face)
        
        # Draw edges
        edge_color = darken_color(self.base_color, 0.4)
        pygame.draw.lines(self.image, edge_color, True, top_face, 2)
        pygame.draw.lines(self.image, edge_color, True, left_face, 2)
        pygame.draw.lines(self.image, edge_color, True, right_face, 2)
        
        # Draw dashed lines for hidden edges
        def draw_dashed_line(surface, color, start_pos, end_pos, width=1, dash_length=4):
            dx = end_pos[0] - start_pos[0]
            dy = end_pos[1] - start_pos[1]
            distance = math.sqrt(dx * dx + dy * dy)
            dashes = int(distance / (2 * dash_length))
            for i in range(dashes):
                start = (
                    start_pos[0] + (dx * i * 2 * dash_length) / distance,
                    start_pos[1] + (dy * i * 2 * dash_length) / distance
                )
                end = (
                    start_pos[0] + (dx * (i * 2 + 1) * dash_length) / distance,
                    start_pos[1] + (dy * (i * 2 + 1) * dash_length) / distance
                )
                pygame.draw.line(surface, color, start, end, width)

        # Draw dashed lines for hidden edges
        hidden_edges = [
            (left_face[3], right_face[1]),  # Back bottom edge
            (left_face[3], left_face[2]),   # Back left edge
            (right_face[1], right_face[2])  # Back right edge
        ]
        for start, end in hidden_edges:
            draw_dashed_line(self.image, edge_color, start, end, 1)

        # Add label
        font = pygame.font.SysFont("Arial", 14, bold=True)
        text_surface = font.render(self.label_text, True, WHITE)
        text_rect = text_surface.get_rect(center=(center_x, center_y))
        self.image.blit(text_surface, text_rect)

    def draw(self, surface):
        # Calculate screen position using same conversion as selection highlight
        iso_x, iso_y = to_isometric(self.grid_x, self.grid_y)
        screen_x = iso_x + MAIN_AREA_WIDTH // 2 - self.rect.width//2
        screen_y = iso_y + SCREEN_HEIGHT // 2 - self.rect.height//2
        
        # Draw shadow
        shadow_points = [
            (self.rect.width//2, self.rect.height//2 + TILE_HEIGHT//2),
            (self.rect.width//2 + TILE_WIDTH//2, self.rect.height//2),
            (self.rect.width//2, self.rect.height//2 - TILE_HEIGHT//2),
            (self.rect.width//2 - TILE_WIDTH//2, self.rect.height//2)
        ]
        
        # Create a surface for the shadow
        shadow_surface = pygame.Surface((self.rect.width, self.rect.height), pygame.SRCALPHA)
        pygame.draw.polygon(shadow_surface, (0, 0, 0, 40), shadow_points)
        
        # Draw shadow and entity
        surface.blit(shadow_surface, (screen_x + 2, screen_y + 2))
        surface.blit(self.image, (screen_x, screen_y))
        
        # Update rect position for collision detection
        self.rect.x = screen_x
        self.rect.y = screen_y

    def update_status_color(self, new_color):
        if self.base_color != new_color:
            self.base_color = new_color
            self.redraw()

    def draw_connections(self, surface):
        pass 

# Modify the PowerSupplier class to use configurations
class PowerSupplier(Entity):
    def __init__(self, grid_x, grid_y, bus_name):
        super().__init__(grid_x, grid_y, SUPPLIER_SIZE, GREEN, "S", bus_name, "supplier")
        self.connected_consumers = []
        self.total_p_demand = 0
        self.total_q_demand = 0
        self.voltage_pu = 1.0
        
        # Use configured values
        if config_manager:
            self.p_nom = config_manager.current_configs["SUPPLIER"]["p_nom_mw"]
            self.v_nom = config_manager.current_configs["SUPPLIER"]["v_nom_kv"]

    def update(self):
        # Calculate total demands - exactly as in pyPSA.py
        self.total_p_demand = sum(c.p_demand_rate for c in self.connected_consumers if c.is_connected)
        self.total_q_demand = sum(c.q_demand_rate for c in self.connected_consumers if c.is_connected)
        
        # Calculate loading percentage - based only on P
        loading_percent = self.total_p_demand / DEFAULT_SUPPLIER_P_CAPACITY
        
        # Simple voltage calculation - exact formula from pyPSA.py
        self.voltage_pu = 1.0 - (loading_percent * 0.1) - (abs(self.total_q_demand) * 0.02)
        
        # Direct threshold-based color updates - no interpolation
        if loading_percent > 0.9 or self.voltage_pu < 0.95:
            self.update_status_color(RED)
            if loading_percent > 0.9:
                add_log_message(f"Warning: High loading at supplier: {loading_percent*100:.1f}%")
            if self.voltage_pu < 0.95:
                add_log_message(f"Warning: Low voltage at supplier: {self.voltage_pu:.2f} pu")
        elif loading_percent > 0.7 or self.voltage_pu < 0.98:
            self.update_status_color(YELLOW)
        else:
            self.update_status_color(GREEN)
        
        self.redraw()

    def connect_consumer(self, consumer):
        if consumer not in self.connected_consumers:
            self.connected_consumers.append(consumer)
            add_log_message(f"Supplier connected to {consumer.label_text} (P={consumer.p_demand_rate:.1f}MW, Q={consumer.q_demand_rate:.1f}MVAr)")
            self.update()

class PowerConsumer(Entity):
    def __init__(self, grid_x, grid_y, bus_name, consumer_type):
        # Use descriptive label: CI, CC, CR
        label = "CI" if consumer_type == CONSUMER_TYPE_INDUCTIVE else ("CC" if consumer_type == CONSUMER_TYPE_CAPACITIVE else "CR")
        
        super().__init__(grid_x, grid_y, CONSUMER_SIZE, BLUE, label, bus_name, "consumer")
        self.consumer_type = consumer_type
        
        # Use configured values
        if config_manager:
            config_type = f"{consumer_type.upper()}_CONSUMER"
            self.p_demand_rate = config_manager.current_configs[config_type]["p_demand_rate"]
            self.power_factor = config_manager.current_configs[config_type]["power_factor"]
            
            # Calculate Q based on power factor
            if consumer_type == CONSUMER_TYPE_INDUCTIVE:
                self.q_demand_rate = self.p_demand_rate * math.tan(math.acos(self.power_factor))
            elif consumer_type == CONSUMER_TYPE_CAPACITIVE:
                self.q_demand_rate = -self.p_demand_rate * math.tan(math.acos(self.power_factor))
            else:  # Resistive
                self.q_demand_rate = 0
        else:
            # Use default values if config_manager not initialized
            self.p_demand_rate = DEFAULT_CONSUMER_P_DEMAND
            if consumer_type == CONSUMER_TYPE_INDUCTIVE:
                self.power_factor = DEFAULT_INDUCTIVE_PF
                self.q_demand_rate = self.p_demand_rate * math.tan(math.acos(DEFAULT_INDUCTIVE_PF))
            elif consumer_type == CONSUMER_TYPE_CAPACITIVE:
                self.power_factor = DEFAULT_CAPACITIVE_PF
                self.q_demand_rate = -self.p_demand_rate * math.tan(math.acos(DEFAULT_CAPACITIVE_PF))
            else:  # Resistive
                self.power_factor = DEFAULT_RESISTIVE_PF
                self.q_demand_rate = 0
        
        self.connected_to = None
        self.is_connected = False

    def connect(self, supplier):
        if supplier is not None:
            self.connected_to = supplier
            self.is_connected = True
            supplier.connect_consumer(self)
            if network is not None:
                try:
                    network.add("Line", f"line_{self.bus_name}",
                            bus0=supplier.bus_name,
                            bus1=self.bus_name,
                            x=0.1,
                            r=0.01)
                    # Update load in network
                    load_name = f"Load_{self.bus_name}"
                    if load_name in network.loads.index:
                        network.loads.loc[load_name, "p_set"] = self.p_demand_rate
                        network.loads.loc[load_name, "q_set"] = self.q_demand_rate
                        if isinstance(network.loads_t.p_set, pd.DataFrame):
                            network.loads_t.p_set[load_name] = self.p_demand_rate
                            network.loads_t.q_set[load_name] = self.q_demand_rate
                except Exception as e:
                    add_log_message(f"Failed to update network: {str(e)}")
            add_log_message(f"Connected {self.label_text} (P={self.p_demand_rate:.1f}MW, Q={self.q_demand_rate:.1f}MVAr)")

    def disconnect(self):
        if self.connected_to is not None:
            if self in self.connected_to.connected_consumers:
                self.connected_to.connected_consumers.remove(self)
            self.connected_to = None
            self.is_connected = False
            if network is not None:
                try:
                    network.remove("Line", f"line_{self.bus_name}")
                    # Reset load in network
                    load_name = f"Load_{self.bus_name}"
                    if load_name in network.loads.index:
                        network.loads.loc[load_name, "p_set"] = 0
                        network.loads.loc[load_name, "q_set"] = 0
                        if isinstance(network.loads_t.p_set, pd.DataFrame):
                            network.loads_t.p_set[load_name] = 0
                            network.loads_t.q_set[load_name] = 0
                except Exception as e:
                    add_log_message(f"Failed to update network: {str(e)}")
            add_log_message(f"Disconnected {self.label_text}")
            self.update_status_color(BLUE)

    def update(self):
        if not self.is_connected:
            self.update_status_color(BLUE)  # Original color
        elif self.connected_to:
            # Direct voltage from supplier - no additional drops
            voltage = self.connected_to.voltage_pu
            
            # Direct threshold-based color updates - exact thresholds from pyPSA.py
            if voltage < 0.95:
                self.update_status_color(RED)
                add_log_message(f"Low voltage at {self.label_text}: {voltage:.2f} pu")
            elif voltage < 0.98:
                self.update_status_color(YELLOW)
            else:
                self.update_status_color(GREEN)
        self.redraw()

    def draw_connections(self, surface):
        if self.connected_to:
            start_pos = (self.rect.x + self.rect.width//2, self.rect.y + self.rect.height//2)
            end_pos = (self.connected_to.rect.x + self.connected_to.rect.width//2,
                      self.connected_to.rect.y + self.connected_to.rect.height//2)
            
            # Exact voltage-based color calculation from pyPSA.py
            voltage = self.connected_to.voltage_pu
            if voltage >= 0.98:
                line_color = GREEN
            elif voltage >= 0.95:
                # Safe interpolation between yellow and green
                factor = min(max((voltage - 0.95) / 0.03, 0), 1)  # Clamp factor between 0 and 1
                green = 255
                red = int(255 * (1-factor))
                line_color = (red, green, 0)
            else:
                # Safe interpolation between red and yellow
                factor = min(max(voltage / 0.95, 0), 1)  # Clamp factor between 0 and 1
                red = 255
                green = int(255 * factor)
                line_color = (red, green, 0)
            
            # Draw connection line with fixed thickness
            pygame.draw.line(surface, line_color, start_pos, end_pos, CONNECTION_THICKNESS)

def initialize_network():
    global network
    try:
        network = pypsa.Network()
        
        # Update time index to use 'h' instead of 'H'
        snapshots = pd.date_range('2024-01-01', periods=1, freq='h')
        network.set_snapshots(list(snapshots))
        
        # Add a reference bus (slack bus) for power flow calculations
        network.add("Bus", "Slack", v_nom=110)
        network.add("Generator", "Slack_Gen",
                    bus="Slack",
                    control="Slack",
                    p_nom=DEFAULT_SUPPLIER_P_CAPACITY,
                    p_set=0)
        
        add_log_message("Network initialized with slack bus")
    except Exception as e:
        add_log_message(f"Failed to initialize network: {str(e)}")

def add_supplier_to_network(grid_x, grid_y):
    global supplier_entity, network
    if supplier_entity is not None:
        add_log_message("Only one supplier allowed")
        return None
    
    if network is None:
        add_log_message("Network not initialized")
        return None
    
    # Create a unique bus name for the supplier
    bus_name = f"Supplier_Bus_{len(network.buses)}"
    
    # Add bus to PyPSA network if it doesn't exist
    if bus_name not in network.buses.index:
        network.add("Bus", bus_name, v_nom=110)
    
    # Add generator to the bus
    gen_name = f"Generator_{bus_name}"
    if gen_name not in network.generators.index:
        network.add(
            "Generator",
            gen_name,
            bus=bus_name,
            p_nom=DEFAULT_SUPPLIER_P_CAPACITY,
            control="PV",  # Voltage-controlled generator
            p_set=DEFAULT_SUPPLIER_P_GENERATION,
            v_set=1.0  # Per unit voltage setpoint
        )
    
    # Create PyGame entity - pass grid coordinates directly
    supplier_entity = PowerSupplier(grid_x, grid_y, bus_name)
    pygame_entities.append(supplier_entity)
    
    add_log_message(f"Added supplier at bus {bus_name}")
    return supplier_entity

def add_consumer_to_network(grid_x, grid_y, consumer_type):
    if network is None:
        add_log_message("Network not initialized")
        return None
        
    # Create a unique bus name for the consumer
    bus_name = f"Consumer_Bus_{len(network.buses)}"
    
    # Add bus to PyPSA network
    if bus_name not in network.buses.index:
        network.add("Bus", bus_name, v_nom=110)
    
    # Create consumer entity - pass grid coordinates directly
    consumer = PowerConsumer(grid_x, grid_y, bus_name, consumer_type)
    pygame_entities.append(consumer)
    
    # Add load to network with proper time series data
    load_name = f"Load_{bus_name}"
    if load_name not in network.loads.index:
        network.add("Load", load_name,
                   bus=bus_name,
                   p_set=consumer.p_demand_rate,
                   q_set=consumer.q_demand_rate)
        
        # Ensure time series data exists
        if isinstance(network.loads_t.p_set, pd.DataFrame):
            network.loads_t.p_set[load_name] = consumer.p_demand_rate
            network.loads_t.q_set[load_name] = consumer.q_demand_rate
    
    add_log_message(f"Consumer {len(network.loads)} added at bus {bus_name} (P: {consumer.p_demand_rate:.1f} MW, Q: {consumer.q_demand_rate:.1f} MVAr)")
    return consumer

def update_network():
    try:
        if network is None:
            return
            
        if len(network.buses) > 0:  # Only run power flow if we have buses
            # Ensure all loads have proper time series data
            for load_name in network.loads.index:
                if isinstance(network.loads_t.p_set, pd.DataFrame):
                    if load_name not in network.loads_t.p_set.columns:
                        network.loads_t.p_set[load_name] = network.loads.loc[load_name, 'p_set']
                    if load_name not in network.loads_t.q_set.columns:
                        network.loads_t.q_set[load_name] = network.loads.loc[load_name, 'q_set']
            
            # Run power flow
            network.pf()
            
            # Update all entities with new power flow results
            for entity in pygame_entities:
                entity.update()
                
    except Exception as e:
        #add_log_message(f"Power flow calculation failed: {str(e)}")
        pass
        # Don't reinitialize network here as it breaks connections

def reset_simulation():
    global network, pygame_entities, supplier_entity, connecting_consumer, temp_connection_line_end
    # Clear all entities
    pygame_entities.clear()
    supplier_entity = None
    connecting_consumer = None
    temp_connection_line_end = None
    # Reinitialize network
    initialize_network()
    add_log_message("Simulation reset - Board cleared")

def draw_menu(screen, menu_items, selected_index, mouse_pos=None):
    # Draw title
    title_font = pygame.font.SysFont("Arial", MENU_TITLE_SIZE)
    title_surface = title_font.render("Energy City Simulator", True, MENU_TITLE_COLOR)
    title_rect = title_surface.get_rect(center=(SCREEN_WIDTH // 2, MENU_START_Y - 100))
    screen.blit(title_surface, title_rect)
    
    # Draw menu items
    menu_font = pygame.font.SysFont("Arial", MENU_ITEM_SIZE)
    for i, item in enumerate(menu_items):
        text_color = MENU_SELECTED_COLOR if i == selected_index else MENU_ITEM_COLOR
        text_surface = menu_font.render(item, True, text_color)
        
        # Create button rectangle
        button_rect = pygame.Rect(
            (SCREEN_WIDTH - MENU_BUTTON_WIDTH) // 2,
            MENU_START_Y + i * (MENU_BUTTON_HEIGHT + MENU_BUTTON_PADDING),
            MENU_BUTTON_WIDTH,
            MENU_BUTTON_HEIGHT
        )
        
        # Draw button background
        pygame.draw.rect(screen, MENU_BG, button_rect)
        
        # Center text in button
        text_rect = text_surface.get_rect(center=button_rect.center)
        screen.blit(text_surface, text_rect)
        
        # Highlight on hover
        if mouse_pos and button_rect.collidepoint(mouse_pos):
            pygame.draw.rect(screen, MENU_ITEM_COLOR, button_rect, 2)

# Modify the menu() function to include configuration option
def menu():
    menu_items = ["Start Simulation", "Configure Components", "Exit"]
    selected_index = 0
    
    while True:
        mouse_pos = pygame.mouse.get_pos()
        
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return "exit"
            
            if event.type == pygame.MOUSEBUTTONDOWN:
                # Calculate button positions
                for i in range(len(menu_items)):
                    button_rect = pygame.Rect(
                        (SCREEN_WIDTH - MENU_BUTTON_WIDTH) // 2,
                        MENU_START_Y + i * (MENU_BUTTON_HEIGHT + MENU_BUTTON_PADDING),
                        MENU_BUTTON_WIDTH,
                        MENU_BUTTON_HEIGHT
                    )
                    if button_rect.collidepoint(mouse_pos):
                        if i == 0:  # Start Simulation
                            return "start"
                        elif i == 1:  # Configure Components
                            return "configure"
                        else:  # Exit
                            return "exit"
            
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_UP:
                    selected_index = (selected_index - 1) % len(menu_items)
                elif event.key == pygame.K_DOWN:
                    selected_index = (selected_index + 1) % len(menu_items)
                elif event.key == pygame.K_RETURN:
                    if selected_index == 0:
                        return "start"
                    elif selected_index == 1:
                        return "configure"
                    else:
                        return "exit"
        
        # Draw menu
        screen = pygame.display.get_surface()
        screen.fill(BLACK)
        draw_menu(screen, menu_items, selected_index, mouse_pos)
        pygame.display.flip()
        
        clock = pygame.time.Clock()
        clock.tick(FPS)

def show_help_screen():
    """Show help screen with controls and information."""
    help_surface = pygame.Surface((SCREEN_WIDTH, SCREEN_HEIGHT))
    help_surface.fill(HELP_BG)
    
    # Draw help text
    font = pygame.font.SysFont("Arial", 16)
    y = 50
    for line in HELP_TEXT.split('\n'):
        if line.strip():  # Only render non-empty lines
            if line.endswith(':'):  # Section headers
                text_surface = font.render(line, True, WHITE)
            else:
                text_surface = font.render(line, True, MENU_TEXT)
            help_surface.blit(text_surface, (50, y))
        y += 25
    
    screen.blit(help_surface, (0, 0))
    pygame.display.flip()
    
    # Wait for user to close help screen
    waiting = True
    while waiting:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return "exit"
            if event.type == pygame.KEYDOWN or event.type == pygame.MOUSEBUTTONDOWN:
                waiting = False
                break

def draw_log_panel(surface):
    pygame.draw.rect(surface, LOG_PANEL_BG, LOG_PANEL_RECT)
    pygame.draw.line(surface, WHITE, (LOG_PANEL_RECT.left, LOG_PANEL_RECT.top), (LOG_PANEL_RECT.right, LOG_PANEL_RECT.top), 1)
    
    if log_font is None:
        return
    
    start_y = LOG_PANEL_RECT.top + 5
    line_height = log_font.get_linesize()
    for i, msg in enumerate(list(log_messages)):
        if start_y + i * line_height > LOG_PANEL_RECT.bottom - line_height:
            break
        text_surface = log_font.render(msg, True, LOG_TEXT_COLOR)
        surface.blit(text_surface, (LOG_PANEL_RECT.left + 5, start_y + i * line_height)) 

class SelectionHighlight:
    def __init__(self):
        self.grid_x = 0
        self.grid_y = 0
        self.color = (255, 255, 255, 128)  # Semi-transparent white
        
    def update_position(self, mouse_pos):
        # Convert screen coordinates to grid coordinates
        screen_center_x = MAIN_AREA_WIDTH // 2
        screen_center_y = SCREEN_HEIGHT // 2
        
        # Calculate relative position from screen center
        rel_x = mouse_pos[0] - screen_center_x
        rel_y = mouse_pos[1] - screen_center_y
        
        # Convert to isometric grid coordinates using same formula as Entity class
        self.grid_x = round((rel_x / TILE_WIDTH + rel_y / TILE_HEIGHT) / 2)
        self.grid_y = round((rel_y / TILE_HEIGHT - rel_x / TILE_WIDTH) / 2)
        
    def draw(self, surface):
        if selected_grid_pos is None:
            return
            
        # Create surface for the highlight
        highlight_surface = pygame.Surface((TILE_WIDTH * 2, TILE_WIDTH * 2), pygame.SRCALPHA)
        
        # Calculate center points for the diamond shape
        center_x = highlight_surface.get_width() // 2
        center_y = highlight_surface.get_height() // 2
        
        # Create diamond shape using the same dimensions as Entity class
        points = [
            (center_x, center_y - TILE_HEIGHT//2),  # Top
            (center_x + TILE_WIDTH//2, center_y),   # Right
            (center_x, center_y + TILE_HEIGHT//2),  # Bottom
            (center_x - TILE_WIDTH//2, center_y)    # Left
        ]
        
        # Draw semi-transparent highlight
        pygame.draw.polygon(highlight_surface, self.color, points)
        pygame.draw.polygon(highlight_surface, (255, 255, 255), points, 2)  # White border
        
        # Calculate screen position using same conversion as Entity class
        iso_x, iso_y = to_isometric(self.grid_x, self.grid_y)
        screen_x = iso_x + MAIN_AREA_WIDTH // 2 - highlight_surface.get_width()//2
        screen_y = iso_y + SCREEN_HEIGHT // 2 - highlight_surface.get_height()//2
        
        # Draw the highlight
        surface.blit(highlight_surface, (screen_x, screen_y))

class SidebarComponent:
    def __init__(self, component_type, y):
        self.type = component_type
        self.config = COMPONENT_TYPES[component_type]
        self.rect = pygame.Rect(
            MAIN_AREA_WIDTH + SIDEBAR_ITEM_PADDING,
            y,
            SIDEBAR_ITEM_WIDTH,
            SIDEBAR_ITEM_HEIGHT
        )
        self.is_hovered = False
        
    def handle_event(self, event):
        global selected_grid_pos, selected_component_type
        if event.type == pygame.MOUSEMOTION:
            self.is_hovered = self.rect.collidepoint(event.pos)
        elif event.type == pygame.MOUSEBUTTONDOWN:
            if event.button == 1 and self.rect.collidepoint(event.pos):
                # If we have a selected grid position, place the component immediately
                if selected_grid_pos:
                    if self.type == "SUPPLIER":
                        add_supplier_to_network(selected_grid_pos[0], selected_grid_pos[1])
                    else:
                        consumer_type = None
                        if self.type == "INDUCTIVE_CONSUMER":
                            consumer_type = CONSUMER_TYPE_INDUCTIVE
                        elif self.type == "CAPACITIVE_CONSUMER":
                            consumer_type = CONSUMER_TYPE_CAPACITIVE
                        elif self.type == "RESISTIVE_CONSUMER":
                            consumer_type = CONSUMER_TYPE_RESISTIVE
                        
                        if consumer_type:
                            add_consumer_to_network(selected_grid_pos[0], selected_grid_pos[1], consumer_type)
                    
                    # Clear selection after placing
                    selected_grid_pos = None
                return True
        return False
        
    def draw(self, surface):
        # Draw component card background
        bg_color = SIDEBAR_ITEM_HOVER if self.is_hovered else SIDEBAR_ITEM_BG
        pygame.draw.rect(surface, bg_color, self.rect, border_radius=5)
        
        # Draw component preview (diamond shape)
        preview_size = min(self.rect.height - 20, CUBE_SIZE // 2)  # Make preview smaller
        center_x = self.rect.x + 20 + preview_size
        center_y = self.rect.centery
        
        # Draw diamond shape
        points = [
            (center_x, center_y - preview_size//2),  # Top
            (center_x + preview_size//2, center_y),  # Right
            (center_x, center_y + preview_size//2),  # Bottom
            (center_x - preview_size//2, center_y)   # Left
        ]
        
        # Draw component diamond
        pygame.draw.polygon(surface, self.config["icon_color"], points)
        pygame.draw.polygon(surface, darken_color(self.config["icon_color"], 0.7), points, 2)
        
        # Draw label
        font = pygame.font.SysFont("Arial", 14, bold=True)
        label_surface = font.render(self.config["label"], True, WHITE)
        label_rect = label_surface.get_rect(center=(center_x, center_y))
        surface.blit(label_surface, label_rect)
        
        # Draw description
        desc_font = pygame.font.SysFont("Arial", 16)
        desc_surface = desc_font.render(self.config["description"], True, SIDEBAR_TEXT)
        desc_x = center_x + preview_size + 10
        desc_y = self.rect.centery - desc_surface.get_height() // 2
        surface.blit(desc_surface, (desc_x, desc_y))

class Sidebar:
    def __init__(self):
        self.rect = pygame.Rect(MAIN_AREA_WIDTH, 0, SIDEBAR_WIDTH, SCREEN_HEIGHT)
        self.components = []
        self.initialize_components()
        
    def initialize_components(self):
        y = SIDEBAR_TITLE_HEIGHT
        for component_type in COMPONENT_TYPES:
            self.components.append(
                SidebarComponent(component_type, y)
            )
            y += SIDEBAR_ITEM_HEIGHT + SIDEBAR_ITEM_MARGIN
            
    def handle_event(self, event):
        global selected_component_type
        
        for component in self.components:
            if component.handle_event(event):
                # Deselect all components
                for c in self.components:
                    c.is_selected = False
                # Select this component
                component.is_selected = True
                selected_component_type = component.type
                return True
        return False
    
    def draw(self, surface):
        # Draw sidebar background
        pygame.draw.rect(surface, SIDEBAR_BG, self.rect)
        
        # Draw shadow
        shadow_surface = pygame.Surface((SIDEBAR_SHADOW_WIDTH, SCREEN_HEIGHT), pygame.SRCALPHA)
        shadow_color = (*SIDEBAR_SHADOW_COLOR, SIDEBAR_SHADOW_ALPHA)
        pygame.draw.rect(shadow_surface, shadow_color, shadow_surface.get_rect())
        surface.blit(shadow_surface, (self.rect.x - SIDEBAR_SHADOW_WIDTH, 0))
        
        # Draw title
        font = pygame.font.SysFont("Arial", 24, bold=True)
        title_surface = font.render("Components", True, SIDEBAR_TEXT)
        title_x = self.rect.x + (SIDEBAR_WIDTH - title_surface.get_width()) // 2
        surface.blit(title_surface, (title_x, (SIDEBAR_TITLE_HEIGHT - title_surface.get_height()) // 2))
        
        # Draw components
        for component in self.components:
            component.draw(surface)

def main():
    global screen, log_font, entity_font, menu_font, selected_grid_pos, selected_component_type, config_manager
    
    pygame.init()
    screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
    pygame.display.set_caption("Energy City Simulator")
    clock = pygame.time.Clock()
    
    # Initialize fonts
    log_font = pygame.font.SysFont("Arial", 14)
    entity_font = pygame.font.SysFont("Arial", ENTITY_FONT_SIZE, bold=True)
    menu_font = pygame.font.SysFont("Arial", MENU_ITEM_SIZE)
    
    # Initialize configuration manager
    config_manager = ConfigurationManager('power_grid.db')
    
    # Create selection highlight
    selection_highlight = SelectionHighlight()
    
    # Create sidebar
    sidebar = Sidebar()
    
    # Show start menu
    while True:
        menu_selection = menu()
        if menu_selection == "exit":
            break
        elif menu_selection == "configure":
            # Show configuration screen
            config_screen = ConfigurationScreen(config_manager)
            running = True
            while running:
                for event in pygame.event.get():
                    if event.type == pygame.QUIT:
                        running = False
                        return  # Exit the program
                    elif event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE:
                        running = False
                    else:
                        result = config_screen.handle_event(event)
                        if result == "menu":  # Check for main menu return signal
                            running = False
                            break
                
                config_screen.draw(screen)
                pygame.display.flip()
                clock.tick(FPS)
        elif menu_selection == "start":
            # Initialize simulation
            initialize_network()
            
            # Initialize selection variables
            selected_grid_pos = None
            
            # Create buttons - position them in the main area
            button_start_x = MAIN_AREA_WIDTH - 4 * (BUTTON_SIZE + BUTTON_PADDING)
            help_button = Button(button_start_x + (BUTTON_SIZE + BUTTON_PADDING), BUTTON_PADDING, BUTTON_SIZE, "?", BUTTON_COLOR)
            reset_button = Button(button_start_x + 2 * (BUTTON_SIZE + BUTTON_PADDING), BUTTON_PADDING, BUTTON_SIZE, "R", BUTTON_COLOR)
            exit_button = Button(button_start_x + 3 * (BUTTON_SIZE + BUTTON_PADDING), BUTTON_PADDING, BUTTON_SIZE, "X", BUTTON_COLOR)
            
            running = True
            connecting_consumer = None
            temp_connection_line_end = None
            last_update_time = pygame.time.get_ticks()
            show_help = False
            
            while running:
                current_time = pygame.time.get_ticks()
                keys_pressed = pygame.key.get_pressed()
                
                for event in pygame.event.get():
                    # Handle quit
                    if event.type == pygame.QUIT:
                        running = False
                        continue

                    # Handle button events
                    if help_button.handle_event(event):
                        show_help = not show_help
                    elif reset_button.handle_event(event):
                        reset_simulation()
                    elif exit_button.handle_event(event):
                        running = False
                        continue

                    # Handle sidebar events first (component selection and placement)
                    if sidebar.handle_event(event):
                        continue

                    # Handle mouse clicks for grid selection and connections
                    if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                        mouse_pos = event.pos
                        if mouse_pos[0] < MAIN_AREA_WIDTH:  # Only if clicking in main area
                            # Check if clicking on existing entity for connection
                            entity_clicked = None
                            for entity in pygame_entities:
                                if entity.rect.collidepoint(mouse_pos):
                                    entity_clicked = entity
                                    break
                            
                            if entity_clicked and (keys_pressed[pygame.K_LSHIFT] or keys_pressed[pygame.K_RSHIFT]):
                                if entity_clicked.entity_type == "consumer":
                                    connecting_consumer = entity_clicked
                                    temp_connection_line_end = connecting_consumer.rect.center
                                    add_log_message(f"Connect C{connecting_consumer.bus_name}...")
                            elif connecting_consumer:
                                # Try to connect to supplier
                                if supplier_entity and supplier_entity.rect.collidepoint(mouse_pos):
                                    connecting_consumer.connect(supplier_entity)
                                else:
                                    # Check if clicked empty space or different entity
                                    if connecting_consumer.connected_to:
                                        connecting_consumer.disconnect()
                                connecting_consumer = None
                                temp_connection_line_end = None
                            else:
                                # Normal grid selection for placement
                                selection_highlight.update_position(mouse_pos)
                                selected_grid_pos = (selection_highlight.grid_x, selection_highlight.grid_y)
                    
                    # Handle mouse motion for connection line
                    elif event.type == pygame.MOUSEMOTION:
                        if connecting_consumer:
                            temp_connection_line_end = event.pos
                    
                    # Handle keyboard
                    elif event.type == pygame.KEYDOWN:
                        if event.key == pygame.K_ESCAPE:
                            if connecting_consumer:
                                connecting_consumer = None
                                temp_connection_line_end = None
                            selected_grid_pos = None
                
                # Update network every second
                if current_time - last_update_time >= 1000:
                    update_network()
                    last_update_time = current_time
                
                # Draw everything
                screen.fill(BLACK)
                
                # Draw all entities
                for entity in pygame_entities:
                    entity.draw_connections(screen)
                    entity.draw(screen)
                
                # Draw temporary connection line
                if connecting_consumer and temp_connection_line_end:
                    pygame.draw.line(screen, CONNECTION_COLOR,
                                   connecting_consumer.rect.center,
                                   temp_connection_line_end,
                                   CONNECTION_THICKNESS)
                
                # Draw selection highlight
                if selected_grid_pos:
                    selection_highlight.grid_x, selection_highlight.grid_y = selected_grid_pos
                    selection_highlight.draw(screen)
                
                # Draw sidebar
                sidebar.draw(screen)
                
                # Draw buttons
                help_button.draw(screen)
                reset_button.draw(screen)
                exit_button.draw(screen)
                
                # Draw log panel
                draw_log_panel(screen)
                
                # Show help screen if active
                if show_help:
                    show_help_screen()
                    show_help = False
                
                pygame.display.flip()
                clock.tick(FPS)
    
    pygame.quit()
    sys.exit()

if __name__ == "__main__":
    main() 
