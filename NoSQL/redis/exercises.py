import json
import time
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


# ==========================================
# Exercise 1: Leaderboard với Sorted Set
# ==========================================
def exercise_1_leaderboard():
    print("--- Exercise 1: Leaderboard ---")
    key = "leaderboard:game1"
    r.delete(key)

    scores = {
        "linh": 1500,
        "nam": 2300,
        "an": 1850,
        "minh": 2750,
        "hoa": 2100,
    }
    r.zadd(key, scores)

    # Cập nhật điểm (tăng thêm cho an)
    r.zincrby(key, 600, "an")  # an -> 2450

    # Lấy top 3 người chơi có điểm cao nhất
    top3 = r.zrevrange(key, 0, 2, withscores=True)
    for rank, (player, score) in enumerate(top3, start=1):
        print(f"Rank #{rank}: {player} ({int(score)} điểm)")


# ==========================================
# Exercise 2: User profiles (Hashes) + Tìm theo thành phố (Sets)
# ==========================================
def add_user(user_id, name, city, role):
    r.hset(f"user:{user_id}", mapping={"name": name, "city": city, "role": role})
    r.sadd(f"city:{city}", user_id)


def find_users_in_city(city):
    user_ids = sorted(r.smembers(f"city:{city}"))
    if not user_ids:
        return []
    pipe = r.pipeline()
    for uid in user_ids:
        pipe.hgetall(f"user:{uid}")
    profiles = pipe.execute()
    return [{"id": uid, **profile} for uid, profile in zip(user_ids, profiles)]


def exercise_2_user_profiles():
    print("\n--- Exercise 2: User Profiles & City Sets ---")
    r.flushdb()
    add_user("u1", "Linh", "Hanoi", "student")
    add_user("u2", "Nam", "Saigon", "engineer")
    add_user("u3", "An", "Hanoi", "designer")
    add_user("u4", "Hoa", "Danang", "analyst")

    hanoi_users = find_users_in_city("Hanoi")
    print("Users ở Hanoi:", hanoi_users)


# ==========================================
# Exercise 3: Đo tốc độ Cache-Aside cho 100 requests lặp lại
# ==========================================
FAKE_DB = {
    "product:1": {"id": 1, "name": "VIP Concert Ticket", "price": 2500000},
}


def fetch_from_db(key):
    time.sleep(0.01)  # Giả lập độ trễ DB 10ms
    return FAKE_DB.get(key)


def get_with_cache(key, ttl=60):
    cached = r.get(f"cache:{key}")
    if cached is not None:
        return json.loads(cached)
    data = fetch_from_db(key)
    if data is not None:
        r.setex(f"cache:{key}", ttl, json.dumps(data))
    return data


def exercise_3_cache_aside():
    print("\n--- Exercise 3: Cache-Aside Speed-up (100 requests) ---")
    r.delete("cache:product:1")
    n_requests = 100

    t0 = time.perf_counter()
    for _ in range(n_requests):
        fetch_from_db("product:1")
    db_time = time.perf_counter() - t0

    t1 = time.perf_counter()
    for _ in range(n_requests):
        get_with_cache("product:1")
    cache_time = time.perf_counter() - t1

    speedup = db_time / cache_time if cache_time > 0 else float("inf")
    print(f"Không dùng cache (DB trực tiếp): {db_time:.4f}s")
    print(f"Có cache-aside (1 miss + 99 hits): {cache_time:.4f}s")
    print(f"Tốc độ nhanh hơn: {speedup:.1f}x")


if __name__ == "__main__":
    exercise_1_leaderboard()
    exercise_2_user_profiles()
    exercise_3_cache_aside()
