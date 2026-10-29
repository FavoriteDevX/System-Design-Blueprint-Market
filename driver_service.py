import json
import time
from redis import Redis
from kafka import KafkaProducer

class DriverTrackingService:
    def __init__(self):
        # Redis Connection for fast Geo-spatial lookups
        self.redis_client = Redis(host='localhost', port=6379, decode_responses=True)
        self.GEO_KEY = "active_drivers_loc"

        # Kafka Producer for streaming location telemetry updates
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.TOPIC_NAME = "driver-telemetry"

    def update_driver_location(self, driver_id: str, lat: float, lon: float):
        """Updates driver location in Redis cache and streams event to Kafka."""
        # 1. Update high-speed cache for real-time querying
        self.redis_client.geoadd(self.GEO_KEY, (lon, lat, driver_id))
        
        # 2. Publish to event pipeline for asynchronous history tracking
        payload = {
            "driver_id": driver_id,
            "latitude": lat,
            "longitude": lon,
            "timestamp": int(time.time())
        }
        self.producer.send(self.TOPIC_NAME, value=payload)
        self.producer.flush()
        print(f"[Telemetry] Driver {driver_id} position updated to ({lat}, {lon})")

    def get_nearby_drivers(self, rider_lat: float, rider_lon: float, radius_km: float = 5.0):
        """Fetches all available drivers within a specific radius using Redis GIS capabilities."""
        raw_results = self.redis_client.georadius(
            name=self.GEO_KEY,
            longitude=rider_lon,
            latitude=rider_lat,
            radius=radius_km,
            unit="km",
            withdist=True,
            sort="ASC"
        )
        
        drivers = [{"driver_id": res[0], "distance_km": res[1]} for res in raw_results]
        return drivers

# Simple Mock Demonstration
if __name__ == "__main__":
    service = DriverTrackingService()
    
    # Simulate Driver movements (Slightly different positions in São Paulo area)
    service.update_driver_location("driver_alpha", -23.550520, -46.633308)
    service.update_driver_location("driver_bravo", -23.556000, -46.639000)
    
    # Query nearby drivers from a specific Rider location
    print("\n[Matching Engine] Searching for closest drivers within 3km radius:")
    nearby = service.get_nearby_drivers(rider_lat=-23.551000, rider_lon=-46.634000, radius_km=3.0)
    print(json.dumps(nearby, indent=2))
