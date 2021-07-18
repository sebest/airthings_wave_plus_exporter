#!/usr/bin/env python3

import time
import click
import asyncio
import struct
import logging
from bleak import BleakScanner, BleakClient
from typing import Optional, List, Dict, Tuple
from aioprometheus import Gauge, Service, Registry

SERVICE_UUID = "b42e1c08-ade7-11e4-89d3-123b93f75cba"
CHAR_UUID = "b42e2a68-ade7-11e4-89d3-123b93f75cba"
MFDATA_SN_UUID = 0x0334

DEFAULT_HTTP_ADDR = "127.0.0.1"
DEFAULT_HTTP_PORT = 9745
# We try to collect metrics every 4 minutes, WavePlus only update
# the metrics every 5 minutes but we want to account for possible
# connection errors that will trigger retries
DEFAULT_UPDATE_METRICS_FREQUENCY = 4 * 60

logger = logging.getLogger("wave_plus_exporter")
logger.setLevel(logging.INFO)
steam_handler = logging.StreamHandler()
steam_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
logger.addHandler(steam_handler)


class WavePlusDevice(object):
    def __init__(self, serial_number, name: Optional[str] = None) -> None:
        self.serial_number = serial_number
        self.name = f"airthings_wave_plus-{name or serial_number}"
        self.address = None
        self.detected = None

    def set_address(self, address: str) -> None:
        self.address = address
        self.detected = self.address != None

    def __repr__(self) -> str:
        return (
            f"WavePlusDevice("
            f"name={self.name}, "
            f"serial_number={self.serial_number}, "
            f"address={self.address}, "
            f"detected={self.detected})"
        )

    async def collect_raw_data(self) -> bytearray:
        async with BleakClient(self.address) as client:
            return await client.read_gatt_char(CHAR_UUID)

    async def collect_metrics(
        self, max_tries: int = 12, sleep_between_retries: int = 5
    ) -> Optional[Tuple[int]]:
        if not self.detected:
            logger.error(
                "%s (sn=%s) was not detected, can't collect metrics",
                self.name,
                self.serial_number,
            )
            return

        logger.debug(
            "collecting metrics from %s (sn=%s)",
            self.name,
            self.serial_number,
        )

        start_time = time.time()
        raw_metrics = None
        exc = None
        for retry in range(max_tries):
            try:
                raw_data = await asyncio.wait_for(self.collect_raw_data(), timeout=15)
                raw_metrics = struct.unpack("BBBBHHHHHHHH", raw_data)
                break
            except Exception as exc:
                logger.warning(
                    "exception while collecting metrics [retry=%d]: %s", retry, exc
                )
                await asyncio.sleep(sleep_between_retries)
        collect_duration = int(time.time() - start_time)

        if raw_metrics:
            logger.debug(
                "collected metrics from %s (sn=%s) in %ds with %d retries: %s",
                self.name,
                self.serial_number,
                collect_duration,
                retry,
                raw_metrics,
            )
        else:
            logger.error(
                "failed to collect metrics from %s (sn=%s) after %ds: %s",
                self.name,
                self.serial_number,
                collect_duration,
                exc,
            )
        return raw_metrics


class WavePlusDeviceDetector(object):
    def __init__(self, scan_duration: int = 10) -> None:
        self.sn_map: Dict[int, str] = {}
        self.scan_duration = scan_duration
        self.scanned = False

    @staticmethod
    def parse_serial_number(data: bytes) -> Optional[int]:
        try:
            serial_number = data[0]
            serial_number |= data[1] << 8
            serial_number |= data[2] << 16
            serial_number |= data[3] << 24
            return serial_number
        except IndexError:
            return None

    async def scan(self) -> None:
        logger.info("scanning for AirThings Wave Plus devices...")
        devices = await BleakScanner.discover(timeout=self.scan_duration)
        for device in devices:
            uuids = device.metadata.get("uuids", [])
            if SERVICE_UUID in uuids:
                try:
                    data = device.metadata["manufacturer_data"][MFDATA_SN_UUID]
                    serial_number = self.parse_serial_number(data)
                    if serial_number:
                        self.sn_map[serial_number] = device.address
                        logger.info(
                            "found %s with serial number %d",
                            device.address,
                            serial_number,
                        )
                except KeyError:
                    continue
        logger.info("%d device(s) found", len(self.sn_map))
        self.scanned = True

    def get_address(self, device: WavePlusDevice) -> str:
        return self.sn_map.get(device.serial_number)

    async def detect(self, device: WavePlusDevice) -> WavePlusDevice:
        if not self.scanned:
            await self.scan()
        device.set_address(self.get_address(device))


class WavePlusSensors(object):

    HUMID = "humidity_percent"
    RADON_S = "radon_short_term_avg_becquerels"
    RADON_L = "radon_long_term_avg_becquerels"
    TEMP = "temperature_celsius"
    PRESS = "pressure_pascal"
    CO2 = "carbondioxide_ppm"
    VOC = "voc_ppb"

    def __init__(self) -> None:
        self.registry = Registry()
        counters = {
            self.HUMID: r"Humidity (%rH)",
            self.RADON_S: "Radon short term average (Bq/m3)",
            self.RADON_L: "Radon long term average (Bq/m3)",
            self.TEMP: "Temperature (°C)",
            self.PRESS: "Relative atmospheric pressure (hPa)",
            self.CO2: "CO2 Level (ppm)",
            self.VOC: "TVOC level (ppb)",
        }
        for name, desc in counters.items():
            self.registry.register(Gauge(name, desc))

    def update_counters(self, name, raw_metrics) -> None:
        if raw_metrics[0] != 1:
            logger.error("invalid metrics: version=%s is not equal 1", raw_metrics[0])
            return
        self.registry.get(self.HUMID).set({"name": name}, raw_metrics[1] / 2.0)
        self.registry.get(self.RADON_S).set({"name": name}, self._radon(raw_metrics[4]))
        self.registry.get(self.RADON_L).set({"name": name}, self._radon(raw_metrics[5]))
        self.registry.get(self.TEMP).set({"name": name}, raw_metrics[6] / 100.0)
        self.registry.get(self.PRESS).set({"name": name}, raw_metrics[7] / 50.0)
        self.registry.get(self.CO2).set({"name": name}, raw_metrics[8] * 1.0)
        self.registry.get(self.VOC).set({"name": name}, raw_metrics[9] * 1.0)

    @staticmethod
    def _radon(radon: int) -> int:
        if 0 <= radon <= 16383:
            return radon
        # Either invalid measurement, or not available
        return -1


class WavePlusExporter(object):
    def __init__(
        self,
        devices: List[WavePlusDevice],
        http_addr: str = DEFAULT_HTTP_ADDR,
        http_port: int = DEFAULT_HTTP_PORT,
        update_metrics_frequency: int = DEFAULT_UPDATE_METRICS_FREQUENCY,
    ) -> None:
        self.devices = devices
        self.http_addr = http_addr
        self.http_port = http_port
        self.update_metrics_frequency = update_metrics_frequency

        self.sensors = WavePlusSensors()

    async def update_metrics(self) -> None:
        while True:
            for device in self.devices:
                raw_metrics = await device.collect_metrics()
                if raw_metrics:
                    self.sensors.update_counters(device.name, raw_metrics)
            await asyncio.sleep(self.update_metrics_frequency)

    async def start(self) -> None:
        # Detect devices and set their address
        detector = WavePlusDeviceDetector()
        for device in self.devices:
            await detector.detect(device)

        svc = Service(self.sensors.registry)
        await svc.start(addr=self.http_addr, port=self.http_port)
        logger.info("Serving prometheus metrics on: %s", svc.metrics_url)

        await self.update_metrics()


@click.command()
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging",
)
@click.option(
    "--name",
    default="",
    help="Name or location of the device, if not provided the serial number is used",
)
@click.option(
    "--http_addr", default=DEFAULT_HTTP_ADDR, help="IP for the HTTP server to listen on"
)
@click.option(
    "--http_port",
    default=DEFAULT_HTTP_PORT,
    help="Port for the HTTP server to listen on",
)
@click.argument("serial_number", type=int)
def main(
    debug: bool, name: str, http_addr: str, http_port: int, serial_number: int
) -> None:
    if debug:
        logger.setLevel(logging.DEBUG)

    devices = [
        WavePlusDevice(serial_number, name),
    ]
    exporter = WavePlusExporter(devices, http_addr, http_port)
    asyncio.run(exporter.start())


if __name__ == "__main__":
    main()
