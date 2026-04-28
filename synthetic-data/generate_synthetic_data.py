#!/usr/bin/env python3
"""
Arrow Retail AI Assistant - Synthetic Electronic Components Dataset Generator
=============================================================================

Generates a CSV of realistic Arrow Electronics-style components that matches
the schema of ``shared/data/products.csv``:

    category, subcategory, name, description, url, price, image

The pipeline uses an OpenAI-compatible LLM API. **Default** is **local NIM**
on the host: ``llm_port`` from ``shared/configs/chain_server/config.yaml``
(``http://llama:8000/v1`` is rewritten to ``http://127.0.0.1:8000/v1``), or
``http://127.0.0.1:8000/v1`` if the file is missing. Use ``--nim-cloud`` or
``LLM_BASE_URL=https://integrate.api.nvidia.com/v1`` for NVIDIA NIM cloud (see
``resolve_llm_base_url()``).

Usage
-----
    # Quick run with defaults (~100 rows, local NIM — start docker-compose-nim-local first)
    export NGC_API_KEY=nvapi-...
    python3 generate_electronic_components.py

    # NVIDIA NIM cloud instead of local
    export NGC_API_KEY=nvapi-...
    python3 generate_electronic_components.py --nim-cloud

    # Larger dataset
    python3 generate_electronic_components.py \
        --count 500 \
        --output ../shared/data/electronic_components.csv

    # Photo-friendly catalog (finished products; pair with fetch_component_images.py
    # --query-suffix for best image hit rate)
    python3 generate_electronic_components.py --preset visual --count 100

    # Use OpenAI instead of NVIDIA NIM
    export OPENAI_API_KEY=sk-...
    python3 generate_electronic_components.py \
        --base-url https://api.openai.com/v1 \
        --model gpt-4o-mini

Re-running the script is safe: rows already present in the output CSV are kept
and only the remaining rows are generated.
"""
from __future__ import annotations

import argparse
import csv
import email.utils
import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("gen_components")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _parse_yaml_scalar(path: Path, key: str) -> str | None:
    """Read a simple ``key: value`` line from a YAML file (no PyYAML)."""
    if not path.is_file():
        return None
    prefix = f"{key}:"
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith(prefix):
            rest = stripped[len(prefix) :].strip()
            if not rest:
                return None
            return rest.strip("\"'")
    return None


def _hostify_llm_url(url: str) -> str:
    """Replace Docker Compose service hostnames with localhost for host-side runs."""
    url = url.strip().rstrip("/")
    for hostname in ("llama", "embedqa", "nvclip"):
        url = re.sub(
            rf"^http://{hostname}:",
            "http://127.0.0.1:",
            url,
            count=1,
        )
        url = re.sub(
            rf"^https://{hostname}:",
            "https://127.0.0.1:",
            url,
            count=1,
        )
    return url


def resolve_llm_base_url(
    base_url_arg: str | None,
    nim_cloud: bool,
) -> str:
    """Pick the chat-completions base URL.

    1. ``--base-url`` if provided
    2. else ``$LLM_BASE_URL`` if set
    3. else if ``--nim-cloud``: ``https://integrate.api.nvidia.com/v1``
    4. else ``llm_port`` from ``shared/configs/chain_server/config.yaml``
       (Docker hostnames mapped to ``127.0.0.1``), or
       ``http://127.0.0.1:8000/v1``
    """
    if base_url_arg:
        return _hostify_llm_url(base_url_arg)
    env = os.environ.get("LLM_BASE_URL", "").strip()
    if env:
        return _hostify_llm_url(env)
    if nim_cloud:
        return "https://integrate.api.nvidia.com/v1"
    cfg = _repo_root() / "shared" / "configs" / "chain_server" / "config.yaml"
    port = _parse_yaml_scalar(cfg, "llm_port")
    if port:
        return _hostify_llm_url(port)
    return "http://127.0.0.1:8000/v1"


def resolve_llm_model(model_arg: str) -> str:
    """If model is still the default, prefer ``llm_name`` from chain_server config."""
    default_model = "meta/llama-3.1-70b-instruct"
    if model_arg != default_model:
        return model_arg
    cfg = _repo_root() / "shared" / "configs" / "chain_server" / "config.yaml"
    name = _parse_yaml_scalar(cfg, "llm_name")
    return name if name else model_arg


# ---------------------------------------------------------------------------
# Arrow Electronics taxonomy
# ---------------------------------------------------------------------------
# Each family describes one generative "bucket": a category/subcategory pair
# plus a list of representative part families and typical manufacturers. The
# LLM samples within these constraints so the dataset stays realistic.

@dataclass(frozen=True)
class Family:
    category: str
    subcategory: str
    part_families: tuple[str, ...]
    manufacturers: tuple[str, ...]
    price_range: tuple[float, float]  # USD, unit price


FAMILIES: tuple[Family, ...] = (
    # --- Semiconductors -----------------------------------------------------
    Family(
        "Semiconductors", "Microcontrollers - MCUs",
        ("8-bit MCU", "16-bit MCU", "32-bit ARM Cortex-M0+ MCU",
         "ARM Cortex-M4F MCU", "ARM Cortex-M7 MCU", "RISC-V MCU",
         "Wireless SoC (BLE)", "Wireless SoC (Wi-Fi)", "Automotive MCU"),
        ("STMicroelectronics", "Microchip Technology", "NXP Semiconductors",
         "Texas Instruments", "Renesas", "Nordic Semiconductor",
         "Silicon Labs", "Infineon Technologies", "Espressif Systems"),
        (0.80, 18.50),
    ),
    Family(
        "Semiconductors", "Microprocessors - MPUs",
        ("ARM Cortex-A7 MPU", "ARM Cortex-A53 MPU", "ARM Cortex-A72 MPU",
         "i.MX Applications Processor", "Sitara MPU"),
        ("NXP Semiconductors", "Texas Instruments", "STMicroelectronics",
         "Renesas", "Rockchip"),
        (5.00, 45.00),
    ),
    Family(
        "Semiconductors", "Operational Amplifiers (Op Amps)",
        ("Single Op-Amp", "Dual Op-Amp", "Quad Op-Amp",
         "Low-Noise Precision Op-Amp", "Rail-to-Rail Op-Amp",
         "High-Speed Op-Amp", "JFET Input Op-Amp"),
        ("Analog Devices", "Texas Instruments", "STMicroelectronics",
         "Microchip Technology", "onsemi", "Maxim Integrated"),
        (0.30, 9.50),
    ),
    Family(
        "Semiconductors", "Memory",
        ("Serial NOR Flash", "NAND Flash", "I2C EEPROM", "SPI EEPROM",
         "FRAM", "SRAM", "LPDDR4 DRAM"),
        ("Micron Technology", "Winbond", "Macronix", "Infineon Technologies",
         "Microchip Technology", "STMicroelectronics", "ISSI"),
        (0.40, 22.00),
    ),
    Family(
        "Semiconductors", "MOSFETs",
        ("N-Channel MOSFET", "P-Channel MOSFET", "Dual MOSFET",
         "Logic-Level MOSFET", "GaN FET", "SiC MOSFET",
         "Automotive MOSFET"),
        ("Infineon Technologies", "onsemi", "Vishay", "Diodes Incorporated",
         "Nexperia", "ROHM", "STMicroelectronics", "Toshiba"),
        (0.12, 6.80),
    ),
    Family(
        "Semiconductors", "Bipolar Transistors",
        ("NPN BJT", "PNP BJT", "Darlington Transistor",
         "Pre-Biased BJT", "RF BJT"),
        ("onsemi", "Nexperia", "Diodes Incorporated", "ROHM",
         "Infineon Technologies"),
        (0.05, 1.20),
    ),
    Family(
        "Semiconductors", "Standard Logic",
        ("Quad 2-Input NAND Gate", "Hex Inverter",
         "Dual D Flip-Flop", "8-bit Shift Register",
         "4-to-16 Line Decoder", "Dual 4-bit Binary Counter",
         "Octal Buffer / Line Driver"),
        ("Texas Instruments", "Nexperia", "onsemi",
         "Diodes Incorporated", "Toshiba"),
        (0.20, 2.80),
    ),
    Family(
        "Semiconductors", "Data Acquisition - ADC",
        ("12-bit SAR ADC", "16-bit Sigma-Delta ADC",
         "24-bit Precision ADC", "High-Speed Pipeline ADC"),
        ("Analog Devices", "Texas Instruments", "Microchip Technology",
         "Maxim Integrated", "STMicroelectronics"),
        (1.80, 28.00),
    ),
    Family(
        "Semiconductors", "Data Acquisition - DAC",
        ("8-bit DAC", "12-bit DAC", "16-bit DAC",
         "Multi-Channel DAC with I2C"),
        ("Analog Devices", "Texas Instruments", "Microchip Technology",
         "Maxim Integrated"),
        (1.50, 22.00),
    ),
    Family(
        "Semiconductors", "Interface - Transceivers",
        ("RS-485 Transceiver", "CAN Transceiver", "LIN Transceiver",
         "USB-to-UART Bridge", "Isolated RS-232 Transceiver"),
        ("Analog Devices", "Texas Instruments", "Maxim Integrated",
         "Microchip Technology", "NXP Semiconductors", "FTDI"),
        (0.90, 7.50),
    ),

    # --- Passives -----------------------------------------------------------
    Family(
        "Passives", "Capacitors - MLCC",
        ("0402 MLCC", "0603 MLCC", "0805 MLCC", "1206 MLCC",
         "High-CV X7R MLCC", "C0G/NP0 MLCC", "High-Voltage MLCC"),
        ("Murata", "TDK", "Samsung Electro-Mechanics", "Yageo",
         "Kemet", "KYOCERA AVX"),
        (0.02, 1.40),
    ),
    Family(
        "Passives", "Capacitors - Aluminum Electrolytic",
        ("Radial Aluminum Electrolytic", "Low-ESR Aluminum Electrolytic",
         "Long-Life 105C Electrolytic", "Surface-Mount Electrolytic"),
        ("Nichicon", "Panasonic", "Rubycon", "Nippon Chemi-Con",
         "Würth Elektronik"),
        (0.08, 3.50),
    ),
    Family(
        "Passives", "Capacitors - Tantalum",
        ("Solid Tantalum Capacitor", "Polymer Tantalum Capacitor",
         "Wet Tantalum Capacitor"),
        ("KYOCERA AVX", "Kemet", "Vishay", "Panasonic"),
        (0.25, 4.80),
    ),
    Family(
        "Passives", "Resistors - Chip",
        ("0402 Thick Film Resistor", "0603 Thick Film Resistor",
         "0805 Thin Film Resistor", "1206 Thick Film Resistor",
         "Precision Thin Film Resistor", "Low-Ohmic Current Sense Resistor"),
        ("Yageo", "Vishay", "Panasonic", "Bourns", "KOA Speer",
         "Susumu", "Rohm"),
        (0.01, 0.75),
    ),
    Family(
        "Passives", "Resistors - Wirewound / Power",
        ("Wirewound Power Resistor", "Aluminum-Housed Power Resistor",
         "TO-220 Power Resistor"),
        ("Vishay", "Bourns", "Ohmite", "TE Connectivity"),
        (0.80, 12.00),
    ),
    Family(
        "Passives", "Inductors",
        ("Shielded Power Inductor", "Unshielded Power Inductor",
         "Wirewound Chip Inductor", "Multilayer Chip Inductor",
         "Common-Mode Choke", "RF Chip Inductor"),
        ("Coilcraft", "Würth Elektronik", "TDK", "Murata", "Bourns",
         "Pulse Electronics", "Vishay"),
        (0.10, 3.20),
    ),
    Family(
        "Passives", "Ferrite Beads",
        ("0402 Ferrite Bead", "0603 Ferrite Bead", "0805 Ferrite Bead",
         "High-Current Ferrite Bead"),
        ("Murata", "TDK", "Würth Elektronik", "Yageo", "Bourns"),
        (0.02, 0.40),
    ),
    Family(
        "Passives", "Crystals and Oscillators",
        ("SMD Quartz Crystal", "32.768 kHz Tuning Fork Crystal",
         "Programmable MEMS Oscillator", "TCXO", "VCXO",
         "Crystal Oscillator"),
        ("Epson", "Abracon", "ECS", "Kyocera", "TXC", "SiTime",
         "IQD Frequency Products"),
        (0.25, 8.50),
    ),

    # --- Power --------------------------------------------------------------
    Family(
        "Power", "Power Management - LDO",
        ("Low-Dropout Linear Regulator", "Ultra-Low Noise LDO",
         "High-PSRR LDO", "Dual-Output LDO"),
        ("Texas Instruments", "Analog Devices", "onsemi",
         "STMicroelectronics", "Microchip Technology", "Diodes Incorporated"),
        (0.40, 4.20),
    ),
    Family(
        "Power", "Power Management - DC-DC",
        ("Synchronous Buck Converter", "Boost Converter",
         "Buck-Boost Converter", "LED Driver IC",
         "Multi-Phase Buck Controller", "PMIC"),
        ("Texas Instruments", "Analog Devices", "Infineon Technologies",
         "STMicroelectronics", "onsemi", "Monolithic Power Systems",
         "Renesas"),
        (1.20, 18.00),
    ),
    Family(
        "Power", "Power Supplies - DC-DC Modules",
        ("Isolated DC-DC Converter", "Non-Isolated Point-of-Load Module",
         "Wide-Input DC-DC Module", "Railway DC-DC Module"),
        ("RECOM", "TRACO Power", "CUI Inc.", "Vicor",
         "Murata Power Solutions", "Delta Electronics"),
        (4.50, 75.00),
    ),
    Family(
        "Power", "Batteries",
        ("Lithium Coin Cell CR2032", "Lithium Coin Cell CR2025",
         "Alkaline AA", "Alkaline 9V", "NiMH Rechargeable AA",
         "Lithium-Polymer Pack", "Industrial Lithium Thionyl Chloride"),
        ("Panasonic", "Energizer", "Duracell", "Renata", "Varta",
         "EVE Energy", "Saft", "Murata"),
        (0.60, 24.00),
    ),

    # --- Connectors ---------------------------------------------------------
    Family(
        "Connectors", "USB Connectors",
        ("USB Type-C Receptacle", "USB 2.0 Type-A Receptacle",
         "Micro USB Type-B Receptacle", "USB 3.2 Type-C Plug"),
        ("Amphenol", "Molex", "TE Connectivity", "Hirose", "JAE",
         "GCT", "Würth Elektronik"),
        (0.40, 4.50),
    ),
    Family(
        "Connectors", "Headers and Wire-to-Board",
        ("2.54 mm Pin Header", "1.27 mm Pin Header",
         "JST PH Through-Hole Header", "JST XH Through-Hole Header",
         "Molex Picoblade Header", "Board-to-Board Socket"),
        ("Molex", "TE Connectivity", "Amphenol", "JST", "Samtec",
         "Harwin", "Würth Elektronik"),
        (0.18, 3.80),
    ),
    Family(
        "Connectors", "Terminal Blocks",
        ("5.08 mm Pluggable Terminal Block",
         "3.5 mm Pluggable Terminal Block",
         "Fixed PCB Terminal Block", "Spring-Clamp Terminal Block"),
        ("Phoenix Contact", "Würth Elektronik", "Molex", "TE Connectivity",
         "On Shore Technology"),
        (0.80, 6.20),
    ),
    Family(
        "Connectors", "RF Connectors",
        ("SMA Edge-Mount Jack", "U.FL Receptacle",
         "BNC Panel-Mount Jack", "MMCX Jack", "N-Type Connector"),
        ("Amphenol RF", "Molex", "Hirose", "Würth Elektronik",
         "TE Connectivity", "Johnson Components"),
        (0.70, 9.50),
    ),

    # --- Sensors ------------------------------------------------------------
    Family(
        "Sensors", "Accelerometers",
        ("3-axis MEMS Accelerometer", "Low-Power 3-axis Accelerometer",
         "Wide-Range Accelerometer", "Automotive Accelerometer"),
        ("STMicroelectronics", "Analog Devices", "Bosch Sensortec",
         "NXP Semiconductors", "TDK InvenSense", "Kionix"),
        (1.20, 12.00),
    ),
    Family(
        "Sensors", "Gyroscopes and IMUs",
        ("6-axis IMU (accel + gyro)", "9-axis IMU",
         "3-axis MEMS Gyroscope", "Industrial IMU Module"),
        ("Bosch Sensortec", "STMicroelectronics", "TDK InvenSense",
         "Analog Devices"),
        (2.50, 35.00),
    ),
    Family(
        "Sensors", "Temperature and Humidity",
        ("I2C Digital Temperature Sensor",
         "Precision Temperature and Humidity Sensor",
         "PT100 RTD Probe", "Thermistor NTC 10k",
         "Digital Thermocouple Interface"),
        ("Sensirion", "Texas Instruments", "Maxim Integrated",
         "Analog Devices", "Microchip Technology", "Vishay",
         "TDK", "Honeywell"),
        (0.45, 14.00),
    ),
    Family(
        "Sensors", "Pressure Sensors",
        ("Barometric Pressure Sensor",
         "Board-Mount Differential Pressure Sensor",
         "Industrial Gauge Pressure Sensor",
         "Absolute Pressure Sensor"),
        ("Bosch Sensortec", "Honeywell", "Amphenol", "TE Connectivity",
         "STMicroelectronics", "Infineon Technologies"),
        (2.80, 38.00),
    ),
    Family(
        "Sensors", "Hall Effect and Magnetic",
        ("Bipolar Hall-Effect Switch", "Linear Hall-Effect Sensor",
         "3-axis Magnetometer", "Magnetic Angle Sensor"),
        ("Allegro MicroSystems", "Melexis", "Infineon Technologies",
         "Texas Instruments", "TDK"),
        (0.70, 6.90),
    ),
    Family(
        "Sensors", "Ambient Light and Proximity",
        ("Ambient Light Sensor", "RGB Color Sensor",
         "Proximity + Ambient Light Sensor", "Time-of-Flight Ranging Sensor"),
        ("ams OSRAM", "STMicroelectronics", "Vishay", "Broadcom",
         "Texas Instruments"),
        (1.00, 8.90),
    ),
    Family(
        "Sensors", "PIR and Motion",
        ("Analog PIR Motion Sensor", "Digital PIR Motion Sensor",
         "Microwave Motion Sensor Module"),
        ("Murata", "Panasonic", "Excelitas Technologies", "Nicera"),
        (2.50, 18.00),
    ),
    Family(
        "Sensors", "Image Sensors",
        ("CMOS Image Sensor 2MP", "CMOS Image Sensor 5MP",
         "Global Shutter CMOS Sensor", "Automotive CMOS Image Sensor"),
        ("onsemi", "OmniVision", "Sony Semiconductor", "STMicroelectronics"),
        (5.80, 48.00),
    ),

    # --- Electromechanical --------------------------------------------------
    Family(
        "Electromechanical", "Switches",
        ("Tactile Switch 6x6 mm", "SPDT Slide Switch",
         "Rotary Encoder Switch", "Toggle Switch",
         "DIP Switch", "Pushbutton Switch with LED"),
        ("C&K", "Omron", "ALPS", "E-Switch", "TE Connectivity",
         "Würth Elektronik", "Bourns", "Panasonic"),
        (0.25, 4.80),
    ),
    Family(
        "Electromechanical", "Relays",
        ("PCB Mount Signal Relay", "Automotive Power Relay",
         "Solid-State Relay", "Latching Relay",
         "High-Voltage Relay"),
        ("Omron", "TE Connectivity", "Panasonic", "Hongfa",
         "Finder", "Fujitsu"),
        (0.90, 14.50),
    ),
    Family(
        "Electromechanical", "Encoders",
        ("Incremental Optical Encoder", "Absolute Magnetic Encoder",
         "Mechanical Rotary Encoder with Detent",
         "Capacitive Encoder"),
        ("CUI Devices", "Bourns", "ALPS", "Broadcom",
         "TT Electronics", "Grayhill"),
        (2.80, 42.00),
    ),

    # --- Circuit Protection -------------------------------------------------
    Family(
        "Circuit Protection", "TVS Diodes and ESD",
        ("Unidirectional TVS Diode", "Bidirectional TVS Diode",
         "USB-Grade ESD Protection Diode",
         "High-Speed Data Line ESD Array"),
        ("Littelfuse", "onsemi", "Diodes Incorporated",
         "Nexperia", "Bourns", "Vishay"),
        (0.10, 1.80),
    ),
    Family(
        "Circuit Protection", "Fuses",
        ("Slow-Blow Fuse 250V", "Fast-Acting Fuse 125V",
         "Resettable PPTC Fuse", "Chip Fuse 0603",
         "Automotive Blade Fuse"),
        ("Littelfuse", "Bel Fuse", "Bourns", "Eaton", "Schurter",
         "SCHOTT"),
        (0.15, 3.20),
    ),
    Family(
        "Circuit Protection", "Varistors and Surge",
        ("Metal Oxide Varistor", "Gas Discharge Tube",
         "Thyristor Surge Suppressor"),
        ("Littelfuse", "Bourns", "Epcos / TDK", "Vishay"),
        (0.25, 3.40),
    ),

    # --- Optoelectronics ----------------------------------------------------
    Family(
        "Optoelectronics", "LEDs - Standard",
        ("Red 0603 SMD LED", "Green 0805 SMD LED",
         "Blue 1206 SMD LED", "White Through-Hole LED",
         "Bi-Color LED", "RGB Common-Anode LED"),
        ("Lite-On", "Broadcom", "Kingbright", "Würth Elektronik",
         "ams OSRAM", "Cree LED", "Stanley Electric"),
        (0.05, 1.40),
    ),
    Family(
        "Optoelectronics", "LEDs - High-Power",
        ("1W High-Power White LED", "3W High-Power Warm-White LED",
         "High-Efficacy COB LED Module", "UV-A High-Power LED"),
        ("Lumileds", "ams OSRAM", "Cree LED", "Nichia", "Seoul Semiconductor"),
        (1.20, 18.00),
    ),
    Family(
        "Optoelectronics", "Infrared and Laser",
        ("IR LED 940 nm", "IR Photodiode",
         "Laser Diode Module 650 nm",
         "Reflective Optical Sensor"),
        ("Vishay", "Broadcom", "ams OSRAM", "Würth Elektronik", "Everlight"),
        (0.35, 9.50),
    ),

    # --- Development Kits and Tools -----------------------------------------
    Family(
        "Development Kits and Tools", "MCU Development Boards",
        ("ARM Cortex-M Nucleo Board", "ESP32 DevKit",
         "Raspberry Pi Pico Board", "STM32 Discovery Kit",
         "PSoC Prototyping Kit", "Arduino-Compatible Board"),
        ("STMicroelectronics", "Espressif Systems", "Raspberry Pi",
         "Infineon Technologies", "Microchip Technology", "NXP Semiconductors"),
        (7.50, 68.00),
    ),
    Family(
        "Development Kits and Tools", "Sensor Evaluation Boards",
        ("IMU Evaluation Board", "Environmental Sensor Dev Kit",
         "ToF Ranging Eval Board", "Current Sense Eval Board"),
        ("STMicroelectronics", "Bosch Sensortec", "Analog Devices",
         "TDK InvenSense", "Texas Instruments"),
        (9.00, 45.00),
    ),
    Family(
        "Development Kits and Tools", "Debug and Programming Probes",
        ("J-Link Debug Probe", "ST-LINK/V3 In-Circuit Debugger",
         "MPLAB PICkit 5 Programmer", "CMSIS-DAP Debug Probe"),
        ("Segger", "STMicroelectronics", "Microchip Technology",
         "NXP Semiconductors"),
        (22.00, 120.00),
    ),
)

# Tier 1 showcase taxonomy only: finished products that read well in a retail
# hero grid (no relays, bare sensors, etc.). Pair with fetch_component_images
# deduplication so identical JPEG bytes are not reused across SKUs.
VISUAL_FAMILIES: tuple[Family, ...] = (
    Family(
        "Computer Products", "Keypads and Keyboards",
        ("Mechanical Gaming Keyboard", "Wireless Low-Profile Keyboard",
         "IP65 Industrial Membrane Keyboard", "Compact TKL Mechanical Keyboard"),
        ("Logitech", "Corsair", "Cherry", "iKey"),
        (45.00, 220.00),
    ),
    Family(
        "Computer Products", "Computer Mouse",
        ("Ergonomic Wireless Mouse", "RGB Gaming Mouse",
         "Trackball Mouse", "Industrial Sealed Mouse"),
        ("Logitech", "Microsoft", "3Dconnexion", "iKey"),
        (25.00, 120.00),
    ),
    Family(
        "Computer Products", "Printers",
        ("Color Laser All-in-One", "Compact Thermal Label Printer",
         "Industrial Barcode Printer", "Office Monochrome Laser Printer"),
        ("Zebra", "Brother", "HP", "Epson", "Honeywell"),
        (120.00, 890.00),
    ),
    Family(
        "Mobile Computing", "Tablet PCs",
        ("10-inch Rugged Android Tablet", "Windows Industrial Tablet",
         "Medical-Grade Tablet with Antimicrobial Housing",
         "Warehouse Handheld Tablet with Scanner"),
        ("Zebra", "Panasonic", "Getac", "Dell", "Honeywell"),
        (380.00, 2200.00),
    ),
    Family(
        "Displays", "TFT Modules",
        ("7-inch Capacitive TFT Module", "10.1-inch IPS HDMI TFT",
         "5-inch Round Automotive TFT", "4.3-inch Sunlight-Readable TFT"),
        ("Newhaven Display", "4D Systems", "Waveshare", "Riverdi", "EastRising"),
        (35.00, 180.00),
    ),
    Family(
        "Displays", "OLED Display Modules",
        ("2.42-inch Graphic OLED", "1.5-inch Full-Color OLED Module",
         "Flexible OLED Wearable Display", "128x64 I2C OLED"),
        ("Newhaven Display", "WiseChip", "Raystar", "BuyDisplay"),
        (12.00, 95.00),
    ),
    Family(
        "Displays", "LCD Monitors",
        ("15-inch Industrial Panel Monitor", "21.5-inch Touch Display Monitor",
         "Sunlight-Readable Marine Display", "Open-Frame Touch Monitor"),
        ("Advantech", "Elo Touch Solutions", "Winmate", "LG Commercial"),
        (220.00, 950.00),
    ),
    Family(
        "LEDs and LED Lighting", "LED Modules",
        ("RGB LED Strip Kit 5m", "COB LED Linear Module",
         "High-CRI LED Panel for Troffers", "Addressable LED Matrix Panel"),
        ("Mean Well", "Philips", "Cree LED", "Adafruit", "SparkFun"),
        (18.00, 140.00),
    ),
    Family(
        "LEDs and LED Lighting", "LED Light Bars, Arrays and Bar Graphs",
        ("Linear LED Light Bar 24V", "Machine-Status LED Stack Light",
         "RGBW LED Bar for Signage", "Emergency Exit LED Light Bar"),
        ("Patlite", "Banner Engineering", "Werma", "Dialight"),
        (35.00, 280.00),
    ),
    Family(
        "LEDs and LED Lighting", "Lighting Fixtures",
        ("LED High-Bay Warehouse Fixture", "Dimmable LED Downlight",
         "Vapor-Tight LED Linear Fixture", "Outdoor LED Floodlight"),
        ("Lithonia", "Cree Lighting", "Acuity Brands", "Philips"),
        (45.00, 420.00),
    ),
    Family(
        "Power Supplies", "External Plug-In Adapters",
        ("12V 3A Desktop Adapter", "5V 4A USB-C PD Adapter",
         "48V PoE Injector Adapter", "9V Center-Negative Adapter"),
        ("CUI Inc.", "Mean Well", "Phihong", "Delta Electronics", "TDK-Lambda"),
        (8.00, 55.00),
    ),
    Family(
        "Power Supplies", "DC to DC Converter and Switching Regulator Module",
        ("Isolated 12V to 5V Brick", "Quarter-Brick 48V to 12V Module",
         "Railway-Grade Wide-Input DC-DC", "1/16 Brick POL Converter"),
        ("RECOM", "TRACO Power", "Vicor", "Murata Power Solutions"),
        (25.00, 220.00),
    ),
    Family(
        "Power Supplies", "Uninterruptible Power Supply Systems - UPS",
        ("1kVA Desktop Line-Interactive UPS", "3kVA Rackmount Online UPS",
         "Compact UPS for Network Closet", "Lithium-Ion Desktop UPS"),
        ("APC", "Eaton", "CyberPower", "Tripp Lite"),
        (120.00, 1800.00),
    ),
    Family(
        "Wire and Cable", "Cable Assembly USB",
        ("USB-C to USB-C 100W Cable", "USB 3.0 Type-A to Micro-B Cable",
         "Right-Angle USB-C Cable", "USB-C to Lightning Cable"),
        ("Tripp Lite", "StarTech", "Belkin", "Anker", "L-com"),
        (6.00, 45.00),
    ),
    Family(
        "Wire and Cable", "Cable Assembly AC Power",
        ("IEC C13 to NEMA 5-15P Cord", "Hospital-Grade Green-Dot Cord",
         "Locking IEC C19 Power Cord", "Schuko to C13 Cord"),
        ("Tripp Lite", "Schurter", "Qualtek", "Volex"),
        (5.00, 35.00),
    ),
    Family(
        "Wire and Cable", "Cable Assembly Coaxial",
        ("LMR-400 N-Male to N-Male Cable", "SMA to RP-SMA Pigtail",
         "BNC RG58 Test Cable Assembly", "Low-Loss Wi-Fi Antenna Cable"),
        ("Times Microwave", "Pasternack", "Amphenol RF", "L-com"),
        (12.00, 95.00),
    ),
    Family(
        "Thermal Management", "Fan Coolers",
        ("120mm PWM Case Fan", "40mm Ball-Bearing Axial Fan",
         "IP55 DC Blower", "1U Rack Fan Tray"),
        ("ebm-papst", "Sunon", "Delta Electronics", "Orion Fans"),
        (8.00, 85.00),
    ),
    Family(
        "Thermal Management", "Heat Sinks",
        ("Extruded Aluminum Profile Heatsink", "Skived Fin Heatsink",
         "Fan-Cooled CPU-Style Heatsink Kit", "Vapor-Chamber Laptop Heatsink"),
        ("Aavid Thermalloy", "Wakefield-Vette", "Fischer Elektronik"),
        (4.00, 75.00),
    ),
    Family(
        "Motors", "Motors Other",
        ("NEMA 17 Bipolar Stepper", "Industrial Brushless Servo Motor",
         "Brushed DC Gearmotor 12V", "Coreless DC Motor for Pump"),
        ("Oriental Motor", "Maxon", "Portescap", "Nanotec", "Faulhaber"),
        (25.00, 380.00),
    ),
    Family(
        "Audio Components", "Speakers",
        ("2-inch Full-Range Driver", "Waterproof PA Horn Speaker",
         "Ceiling Tile Speaker 8 ohm", "USB-Powered Desktop Speaker Pair"),
        ("Visaton", "PUI Audio", "Bose Professional", "JBL Commercial"),
        (12.00, 180.00),
    ),
    Family(
        "Audio Components", "Microphones",
        ("USB Condenser Podcast Microphone", "Gooseneck Conference Mic",
         "Lavalier Tie-Clip Microphone", "Shotgun Mic for Camera Rig"),
        ("Shure", "Sennheiser", "Audio-Technica", "Rode"),
        (45.00, 350.00),
    ),
    Family(
        "Batteries and Accessories", "Batteries",
        ("18650 Lithium-Ion 3000mAh", "12V 7Ah Sealed Lead-Acid",
         "CR123A Lithium Primary 2-Pack", "NiMH AA Rechargeable 4-Pack"),
        ("Panasonic", "Energizer", "EVE Energy", "Yuasa"),
        (6.00, 48.00),
    ),
    Family(
        "Data Storage", "USB Flash Drives",
        ("USB 3.2 Type-A Flash Drive 128GB", "USB-C Metal Flash Drive 64GB",
         "Hardware-Encrypted USB Drive", "Industrial SLC USB Stick"),
        ("SanDisk", "Kingston", "Verbatim", "Corsair", "ADATA"),
        (12.00, 120.00),
    ),
    Family(
        "Test and Measurement", "Multimeters",
        ("True-RMS Digital Multimeter", "Clamp Meter with Bluetooth",
         "Industrial CAT IV Multimeter", "Bench 6.5-Digit Multimeter"),
        ("Fluke", "Keysight", "Hioki", "BK Precision"),
        (45.00, 750.00),
    ),
    Family(
        "Test and Measurement", "Bench Power Supplies",
        ("30V 10A Programmable Bench Supply", "Triple-Output Linear Supply",
         "Compact USB-Programmable PSU", "High-Precision 4-Channel SMU"),
        ("Keysight", "Rohde & Schwarz", "BK Precision", "Siglent"),
        (120.00, 1200.00),
    ),
    Family(
        "Test and Measurement", "Oscilloscopes",
        ("4-Channel 100 MHz Digital Oscilloscope", "Mixed-Signal Oscilloscope",
         "Portable Battery Oscilloscope", "8-Channel MSO for Embedded Debug"),
        ("Keysight", "Tektronix", "Rohde & Schwarz", "Siglent"),
        (350.00, 4500.00),
    ),
    Family(
        "Test and Measurement", "Thermal Imagers",
        ("Handheld Infrared Camera 320x240", "Smartphone Thermal Camera Module",
         "Industrial Thermal Imaging Gun", "Drone-Mount Radiometric Camera"),
        ("FLIR", "Seek Thermal", "Testo", "Hikmicro"),
        (220.00, 3500.00),
    ),
    Family(
        "Development Kits and Tools", "Development Kits and Tools",
        ("STM32 Nucleo Starter Kit", "ESP32 IoT Dev Kit with Sensors",
         "Arduino-Compatible Maker Kit", "Raspberry Pi Pico Learning Kit"),
        ("STMicroelectronics", "Espressif Systems", "Arduino", "Seeed Studio"),
        (25.00, 95.00),
    ),
    Family(
        "Embedded Controllers and Systems", "Single Board Computers - SBCs",
        ("ARM SBC 4GB RAM with Wi-Fi", "Industrial Fanless Box PC",
         "RISC-V SBC with PCIe", "Edge AI SBC with NPU"),
        ("Advantech", "AAEON", "Raspberry Pi", "NVIDIA", "Hardkernel"),
        (45.00, 520.00),
    ),
    Family(
        "Tools", "Soldering Irons and Stations",
        ("Digital Soldering Station 80W", "Portable USB-C Soldering Iron",
         "Hot-Air Rework Station with Preheat"),
        ("Weller", "Hakko", "JBC", "Quick"),
        (60.00, 420.00),
    ),
    Family(
        "RF and Microwave", "Antennas",
        ("2.4/5 GHz Rubber Duck Antenna", "GNSS Active Patch Antenna",
         "LTE MIMO Puck Antenna", "433 MHz Helical Antenna"),
        ("Taoglas", "Pulse Electronics", "Laird", "2J Antennas"),
        (8.00, 95.00),
    ),
    Family(
        "RF and Microwave", "Combo Wireless Modules",
        ("Wi-Fi 6 and Bluetooth Combo Module", "802.15.4 + BLE IoT Module",
         "Industrial Wi-Fi Radio Module with U.FL"),
        ("u-blox", "Silicon Labs", "Murata", "Inventek"),
        (8.00, 48.00),
    ),
    Family(
        "RF and Microwave", "Bluetooth",
        ("BLE 5.3 Module with PCB Antenna", "Classic Bluetooth Audio Module",
         "Dual-Mode Bluetooth Serial Module"),
        ("Silicon Labs", "STMicroelectronics", "Microchip Technology", "Cypress"),
        (4.00, 28.00),
    ),
    Family(
        "Boxes, Enclosures and Racks", "Box, Enclosure and Rack",
        ("ABS Desktop Enclosure", "IP65 Polycarbonate Wall Box",
         "19-inch 2U Rackmount Chassis", "Diecast Aluminum Project Box"),
        ("Hammond Manufacturing", "OKW", "Bopla", "Schroff"),
        (25.00, 240.00),
    ),
    Family(
        "Memory", "Memory Modules",
        ("DDR4 SODIMM 16GB 3200", "DDR5 UDIMM 32GB",
         "Industrial ECC UDIMM", "DDR4 Mini-DIMM Server Memory"),
        ("Micron Technology", "Samsung", "Kingston", "ADATA", "SMART Modular"),
        (35.00, 280.00),
    ),
)


# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a senior product copywriter at Arrow Electronics, a global "
    "distributor of electronic components. Write concise, technically "
    "accurate catalog entries that an engineer can trust. Never invent "
    "part numbers that collide with real vendor SKUs; prefer plausible "
    "synthetic suffixes (for example 'SYN' or a random alphanumeric tail). "
    "Always respond with a single JSON object and nothing else."
)

USER_PROMPT_TEMPLATE = """\
Generate ONE realistic electronic-component catalog entry for Arrow.com.

Constraints
-----------
- Category       : {category}
- Subcategory    : {subcategory}
- Part family    : {part_family}
- Manufacturer   : {manufacturer}
- Target price   : about ${target_price:.2f} USD (single-unit price)
- Must be a distinct product that could realistically be sold by Arrow.

Required JSON schema
--------------------
{{
  "name": "<concise product name, 40-90 chars, includes manufacturer and key spec>",
  "part_number": "<synthetic-but-plausible MPN, 6-14 chars, uppercase/digits/hyphen>",
  "description": "<4-6 sentences. Start with what the part is and its headline spec. Then cover key electrical specs (voltage, current, tolerance, frequency, package, temperature range where relevant). Mention 1-2 typical applications. Close with package/form-factor. No bullet points, no markdown, no line breaks inside the string.>",
  "price": <number, USD, rounded to 2 decimals, within ~40% of the target>,
  "key_specs": "<comma-separated headline specs, e.g. '3.3V, 100mA, 1% tolerance, SOT-23-5'>"
}}

Do not include any text outside the JSON object."""

# Extra constraint when generating the visual / photo-friendly preset.
VISUAL_USER_SUFFIX = """

Additional constraint (visual catalog)
--------------------------------------
The item must be a complete physical product (finished module, cable, tool,
display, fan, battery pack, enclosure, antenna, dev board, etc.) that a shopper
could photograph on a table — not a bare semiconductor die, wafer map, pinout
diagram, schematic, or datasheet excerpt. The name should sound like a retail
SKU title, not an engineering internal code."""


# ---------------------------------------------------------------------------
# LLM client
# ---------------------------------------------------------------------------
def _retry_after_seconds(resp: requests.Response) -> float | None:
    """Parse ``Retry-After`` header (seconds or HTTP-date). Returns None if absent."""
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    raw = raw.strip()
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is not None:
            return max(0.0, (dt.timestamp() - time.time()))
    except (TypeError, ValueError, OSError):
        pass
    return None


def _backoff_for_status(attempt: int, status: int | None, resp: requests.Response | None) -> float:
    """Compute sleep seconds before retry. 429 gets longer waits (NIM is strict)."""
    jitter = random.uniform(0.5, 2.5)
    if status == 429 and resp is not None:
        ra = _retry_after_seconds(resp)
        if ra is not None and ra > 0:
            return ra + jitter
        # No header: exponential cap ~2 minutes
        return min(120.0, 12.0 * (2 ** (attempt - 1))) + jitter
    if status is not None and 500 <= status < 600:
        return min(60.0, 5.0 * (2 ** (attempt - 1))) + jitter
    return min(30.0, 2.0 * (2 ** (attempt - 1))) + jitter


class LLMClient:
    """Minimal OpenAI-compatible chat client (works with NVIDIA NIM)."""

    def __init__(
        self,
        base_url: str,
        model: str,
        api_key: str,
        temperature: float = 0.8,
        max_tokens: int = 600,
        timeout: int = 60,
        max_retries: int = 10,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.max_retries = max_retries

    def chat(self, system: str, user: str) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        url = f"{self.base_url}/chat/completions"

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            resp: requests.Response | None = None
            try:
                resp = requests.post(
                    url, headers=headers, json=payload, timeout=self.timeout
                )
                if resp.status_code == 429:
                    last_err = RuntimeError(
                        f"LLM HTTP 429: {resp.text[:300]}"
                    )
                    backoff = _backoff_for_status(attempt, 429, resp)
                    logger.warning(
                        "Rate limited (attempt %d/%d). Waiting %.1fs before retry.",
                        attempt, self.max_retries, backoff,
                    )
                    time.sleep(backoff)
                    continue
                if 500 <= resp.status_code < 600:
                    last_err = RuntimeError(
                        f"LLM HTTP {resp.status_code}: {resp.text[:300]}"
                    )
                    backoff = _backoff_for_status(attempt, resp.status_code, resp)
                    logger.warning(
                        "Server error (attempt %d/%d): %s. Retrying in %.1fs",
                        attempt, self.max_retries, last_err, backoff,
                    )
                    time.sleep(backoff)
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            except (requests.RequestException, KeyError, IndexError) as exc:
                last_err = exc
                backoff = _backoff_for_status(attempt, None, None)
                logger.warning(
                    "LLM call failed (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt, self.max_retries, exc, backoff,
                )
                time.sleep(backoff)
        assert last_err is not None
        raise last_err


# ---------------------------------------------------------------------------
# Row generation
# ---------------------------------------------------------------------------
_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(text: str) -> dict:
    """Parse the first JSON object found in ``text``.

    Some models wrap JSON in ```json fences or preface it with a sentence;
    strip anything that isn't the outermost object before parsing.
    """
    match = _JSON_RE.search(text)
    if not match:
        raise ValueError(f"No JSON object in response: {text[:200]!r}")
    return json.loads(match.group(0))


def _slugify(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")
    return value[:80] or "component"


@dataclass
class Row:
    category: str
    subcategory: str
    name: str
    description: str
    url: str
    price: float
    image: str

    def as_csv(self) -> list[str]:
        return [
            self.category,
            self.subcategory,
            self.name,
            self.description,
            self.url,
            f"{self.price:.2f}",
            self.image,
        ]


def _compose_row(family: Family, payload: dict) -> Row:
    name = str(payload.get("name", "")).strip()
    description = " ".join(str(payload.get("description", "")).split())
    part_number = str(payload.get("part_number", "")).strip().upper()
    key_specs = str(payload.get("key_specs", "")).strip()

    try:
        price = float(payload.get("price", 0))
    except (TypeError, ValueError):
        price = 0.0
    if price <= 0:
        price = round(random.uniform(*family.price_range), 2)

    if not name or not description:
        raise ValueError("LLM returned empty name or description")

    if key_specs and key_specs.lower() not in description.lower():
        description = f"{description} Key specs: {key_specs}."

    slug = _slugify(f"{family.subcategory}_{part_number or name}")
    url = f"https://www.arrow.com/en/products/{slug.lower()}"
    image = f"/images/{slug}.jpg"

    return Row(
        category=family.category,
        subcategory=family.subcategory,
        name=name,
        description=description,
        url=url,
        price=round(price, 2),
        image=image,
    )


def _build_spec(
    idx: int,
    rng: random.Random,
    families: tuple[Family, ...],
) -> tuple[Family, str, str, float]:
    family = families[idx % len(families)]
    part_family = rng.choice(family.part_families)
    manufacturer = rng.choice(family.manufacturers)
    lo, hi = family.price_range
    target_price = round(rng.uniform(lo, hi), 2)
    return family, part_family, manufacturer, target_price


def _generate_single(
    client: LLMClient,
    family: Family,
    part_family: str,
    manufacturer: str,
    target_price: float,
    *,
    visual_preset: bool,
) -> Row:
    user = USER_PROMPT_TEMPLATE.format(
        category=family.category,
        subcategory=family.subcategory,
        part_family=part_family,
        manufacturer=manufacturer,
        target_price=target_price,
    )
    if visual_preset:
        user = user + VISUAL_USER_SUFFIX
    text = client.chat(SYSTEM_PROMPT, user)
    payload = _extract_json(text)
    return _compose_row(family, payload)


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------
CSV_HEADER = ["category", "subcategory", "name", "description", "url", "price", "image"]


def _load_existing(output: Path) -> list[list[str]]:
    if not output.exists():
        return []
    with output.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    if not rows:
        return []
    if rows[0] == CSV_HEADER:
        rows = rows[1:]
    return rows


def _write_all(output: Path, rows: Iterable[list[str]]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(CSV_HEADER)
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _resolve_api_key(explicit: str | None) -> str:
    if explicit:
        return explicit
    for env in ("NGC_API_KEY", "LLM_API_KEY", "NVIDIA_API_KEY", "OPENAI_API_KEY"):
        val = os.environ.get(env)
        if val:
            logger.info("Using API key from $%s", env)
            return val
    raise SystemExit(
        "No API key found. Set NGC_API_KEY (NVIDIA NIM) or OPENAI_API_KEY, "
        "or pass --api-key."
    )


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parent.parent
    default_output = repo_root / "shared" / "data" / "electronic_components.csv"
    default_visual_output = (
        repo_root / "shared" / "data" / "electronic_components_visual.csv"
    )
    parser = argparse.ArgumentParser(
        description="Generate a synthetic Arrow Electronics components CSV.",
    )
    parser.add_argument(
        "--preset",
        choices=("default", "visual"),
        default="default",
        help="Taxonomy preset: 'default' (broad engineering mix) or 'visual' "
             "(photo-friendly finished products — better for image search).",
    )
    parser.add_argument("--count", type=int, default=100,
                        help="Total number of rows to produce (default: 100)")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path. Default: shared/data/electronic_components.csv "
             "or electronic_components_visual.csv when --preset visual.",
    )
    parser.add_argument("--model", default="meta/llama-3.1-70b-instruct",
                        help="LLM model name (default: meta/llama-3.1-70b-instruct)")
    parser.add_argument(
        "--base-url",
        default=None,
        help="OpenAI-compatible base URL. If omitted: $LLM_BASE_URL, or "
             "--nim-cloud, or local chain_server llm_port / 127.0.0.1:8000.",
    )
    parser.add_argument(
        "--nim-cloud",
        action="store_true",
        help="Use NVIDIA NIM cloud (https://integrate.api.nvidia.com/v1). "
             "Ignored if --base-url or LLM_BASE_URL is set.",
    )
    parser.add_argument(
        "--local-llm",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--api-key", default=None,
                        help="API key (overrides env vars)")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Parallel LLM requests (default: 1). NVIDIA NIM "
                             "often returns HTTP 429 if this is >1.")
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.25,
        help="Seconds to sleep after each successful LLM response (default: "
             "0.25). Helps stay under tokens-per-minute limits.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=10,
        help="Max retries per LLM call for 429/5xx/network errors (default: 10)",
    )
    parser.add_argument("--temperature", type=float, default=0.85,
                        help="Sampling temperature (default: 0.85)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for family/manufacturer sampling")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the taxonomy and exit without calling the LLM")
    args = parser.parse_args()
    if args.output is None:
        args.output = (
            default_visual_output if args.preset == "visual" else default_output
        )
    return args


def main() -> int:
    args = parse_args()

    families: tuple[Family, ...] = (
        VISUAL_FAMILIES if args.preset == "visual" else FAMILIES
    )

    if args.dry_run:
        print(f"preset={args.preset}  families={len(families)}")
        for fam in families:
            print(f"  {fam.category} / {fam.subcategory} "
                  f"({len(fam.part_families)} part families)")
        return 0

    api_key = _resolve_api_key(args.api_key)
    base_url = resolve_llm_base_url(args.base_url, args.nim_cloud)
    model = resolve_llm_model(args.model)
    logger.info("LLM endpoint: %s  model=%s", base_url, model)

    client = LLMClient(
        base_url=base_url,
        model=model,
        api_key=api_key,
        temperature=args.temperature,
        max_retries=max(1, args.max_retries),
    )

    rng = random.Random(args.seed)
    existing = _load_existing(args.output)
    logger.info("Found %d existing rows in %s", len(existing), args.output)

    already_seen = {(r[0], r[2]) for r in existing if len(r) >= 3}
    results: list[list[str]] = list(existing)
    remaining = max(0, args.count - len(existing))

    if remaining == 0:
        logger.info("Target count already met (%d rows). Nothing to do.",
                    args.count)
        return 0

    logger.info("Generating %d new rows with model=%s base_url=%s",
                remaining, model, base_url)

    # Shuffle the FAMILIES order so we don't hammer one category first.
    spec_indices = list(range(remaining))
    rng.shuffle(spec_indices)

    req_delay = max(0.0, args.request_delay)

    visual = args.preset == "visual"

    def task(i: int) -> Row | None:
        fam, part_family, mfr, target_price = _build_spec(i, rng, families)
        try:
            row = _generate_single(
                client, fam, part_family, mfr, target_price, visual_preset=visual
            )
            if row is not None and req_delay > 0:
                time.sleep(req_delay)
            return row
        except Exception as exc:  # noqa: BLE001
            logger.error("Row %d failed (%s / %s / %s): %s",
                         i, fam.subcategory, part_family, mfr, exc)
            return None

    done = 0
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(task, i): i for i in spec_indices}
        for fut in as_completed(futures):
            row = fut.result()
            done += 1
            if row is None:
                continue
            key = (row.category, row.name)
            if key in already_seen:
                logger.info("Skipping duplicate: %s", row.name)
                continue
            already_seen.add(key)
            results.append(row.as_csv())

            # Periodic checkpoint so a crash doesn't lose progress.
            if done % 10 == 0:
                _write_all(args.output, results)
                logger.info("Checkpoint: %d/%d rows written", len(results), args.count)

    _write_all(args.output, results)
    logger.info("Done. Wrote %d rows to %s", len(results), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
