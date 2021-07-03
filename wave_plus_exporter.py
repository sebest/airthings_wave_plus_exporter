#!/usr/bin/env python3

import click
import asyncio
import struct
import logging
from bleak import BleakScanner, BleakClient
from typing import Optional, List, Dict
from aioprometheus import Gauge, Service, Registry

WP_UUID = "b42e1c08-ade7-11e4-89d3-123b93f75cba"
WP_SENSOR_UUID = "b42e2a68-ade7-11e4-89d3-123b93f75cba"
WP_MFDATA_SN_IDX = 820


logger = logging.getLogger("wave_plus_exporter")
logger.setLevel(logging.DEBUG)
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    # level=logging.DEBUG
)


class WavePlusDevice(object):
    def __init__(self, serial_number, name: Optional[str] = None) -> None:
        self.serial_number = serial_number
        self.name = f"airthings_wave_plus-{name or serial_number}"
        self.address = None
        self.detected = None

    def set_address(self, address: str):
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

    async def collect_metrics(self) -> None:
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
        async with BleakClient(self.address) as client:
            raw_data = await client.read_gatt_char(WP_SENSOR_UUID)
            raw_metrics = struct.unpack("BBBBHHHHHHHH", raw_data)

            logger.info(
                "collected metrics from %s (sn=%s): %s",
                self.name,
                self.serial_number,
                raw_metrics,
            )
            return raw_metrics


class WavePlusDeviceDetector(object):
    def __init__(self, timeout: int = 5) -> None:
        self.sn_map: Dict[int, str] = {}
        self.timeout = timeout
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

    async def scan(self):
        logger.info("scanning for AirThings Wave Plus devices...")
        devices = await BleakScanner.discover(timeout=self.timeout)
        for device in devices:
            uuids = device.metadata.get("uuids", [])
            if WP_UUID in uuids:
                try:
                    data = device.metadata["manufacturer_data"][WP_MFDATA_SN_IDX]
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
    def __init__(self) -> None:
        self.registry = Registry()
        self.counters = [
            Gauge("humidity_percent", r"Humidity (%rH)"),
            Gauge("radon_short_term_avg_becquerels", "Radon short term Bq/m3"),
            Gauge("radon_long_term_avg_becquerels", "Radon long term Bq/m3"),
            Gauge("temperature_celsius", "Temperature (degC)"),
            Gauge("pressure_pascal", "Pressure (hPa)"),
            Gauge("carbondioxide_ppm", "CO2 (ppm)"),
            Gauge("voc_ppb", "VOC (ppb)"),
        ]
        for counter in self.counters:
            self.registry.register(counter)

    def update_counters(self, name, raw_metrics):
        if raw_metrics[0] != 1:
            logger.error("invalid metrics: version=%s is not equal 1", raw_metrics[0])
            return
        self.counters[0].set({"name": name}, raw_metrics[1] / 2.0)
        self.counters[1].set({"name": name}, self.conv2radon(raw_metrics[4]))
        self.counters[2].set({"name": name}, self.conv2radon(raw_metrics[5]))
        self.counters[3].set({"name": name}, raw_metrics[6] / 100.0)
        self.counters[4].set({"name": name}, raw_metrics[7] / 50.0)
        self.counters[5].set({"name": name}, raw_metrics[8] * 1.0)
        self.counters[6].set({"name": name}, raw_metrics[9] * 1.0)

    def conv2radon(self, radon):
        if 0 <= radon <= 16383:
            return radon
        # Either invalid measurement, or not available
        return -1


class WavePlusExporter(object):
    def __init__(
        self,
        devices: List[WavePlusDevice],
        http_addr: str = "127.0.0.1",
        http_port: int = 9745,
        update_metrics_frequency: int = 60,
    ) -> None:
        self.devices = devices
        self.http_addr = http_addr
        self.http_port = http_port
        self.update_metrics_frequency = update_metrics_frequency

        self.sensors = WavePlusSensors()

    async def update_metrics(self) -> None:
        while True:
            for device in self.devices:
                try:
                    raw_metrics = await device.collect_metrics()
                    self.sensors.update_counters(device.name, raw_metrics)
                except Exception as exc:
                    logger.error("failed to update metrics: %s", exc)
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
@click.option("--name", default="")
@click.option("--http_addr", default="127.0.0.1")
@click.option("--http_port", default=9745)
@click.argument("serial_number", type=int)
def main(name, http_addr, http_port, serial_number):
    devices = [
        WavePlusDevice(serial_number, name),
    ]
    exporter = WavePlusExporter(devices, http_addr, http_port)
    asyncio.run(exporter.start())


if __name__ == "__main__":
    main()
