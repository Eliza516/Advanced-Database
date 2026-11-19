import time
import uuid
import random
from concurrent.futures import ThreadPoolExecutor
import redis

# Kết nối tới Redis local
r = redis.Redis(host="localhost", 
                port=6379, 
                decode_responses=True)

def race(fn, n=50):
    """Chạy hàm fn(i) đồng thời từ n threads để giả lập nhiều người dùng cùng bấm nút."""
    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(fn, range(n)))

def hold_naive(event, seat, user):
    key = f"hold:{event}:{seat}"
    if r.get(key) is None:
        time.sleep(0.01)  # Mở rộng cửa sổ race để dễ thấy lỗi
        r.set(key, user, ex=300)
        return True
    return False

r.flushdb()
wins_naive = race(lambda i: hold_naive("c1", "A1", f"user {i}"))
print("Số người giữ chỗ thành công với hold_naive (lỗi > 1):", sum(wins_naive))

def hold_nx(event, seat, user, ttl=300):
    # NX: Chỉ đặt key nếu key CHƯA tồn tại
    # EX: Tự động hết hạn (TTL) sau 300 giây
    return bool(r.set(f"hold:{event}:{seat}", user, nx=True, ex=ttl))

# Thử nghiệm kiểm tra:
r.flushdb()
wins = race(lambda i: hold_nx("c1", "A1", f"user {i}"))
print("Số người giữ chỗ thành công (kỳ vọng là 1):", sum(wins))

HOLD = r.register_script("""
if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then return 0 end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3]) then return 1 end
return 0
""")

CONFIRM = r.register_script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SADD', KEYS[2], ARGV[2])
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
""")

RELEASE = r.register_script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
""")

def hold(event, seat, user, ttl=300):
    return bool(HOLD(keys=[f"hold:{event}:{seat}", f"sold:{event}"], args=[user, seat, ttl]))

def confirm(event, seat, user):
    return bool(CONFIRM(keys=[f"hold:{event}:{seat}", f"sold:{event}"], args=[user, seat]))

def release(event, seat, user):
    return bool(RELEASE(keys=[f"hold:{event}:{seat}"], args=[user]))

def seat_map(event, seats):
    sold = r.smembers(f"sold:{event}")
    return {s: "sold" if s in sold else "held" if r.exists(f"hold:{event}:{s}") else "free" for s in seats}

# Kiểm thử kịch bản giữ - xác nhận - bản đồ ghế (Step 2):
r.flushdb()
print("linh hold A1:", hold("c1", "A1", "linh"))          # True
print("nam hold A1:", hold("c1", "A1", "nam"))            # False, linh đang giữ
print("nam confirm A1:", confirm("c1", "A1", "nam"))      # False, nam không sở hữu hold
print("linh confirm A1:", confirm("c1", "A1", "linh"))    # True, đã bán
print("nam hold A1 sau khi bán:", hold("c1", "A1", "nam")) # False, đã bán
print("Bản đồ ghế:", seat_map("c1", ["A1", "A2"]))        # {'A1': 'sold', 'A2': 'free'}

def buy_general(event, user):
    left = r.decr(f"stock:{event}")
    if left < 0:
        r.incr(f"stock:{event}")  # Hoàn lại kho nếu đã hết vé
        return False
    r.sadd(f"buyers:{event}", user)
    return True

# Kiểm thử với 500 người tranh mua 100 vé:
r.flushdb()
r.set("stock:g1", 100)
ok = race(lambda i: buy_general("g1", f"user {i}"), n=500)
print("Số vé bán ra thành công:", sum(ok), "| Kho còn lại:", r.get("stock:g1"), "| Số khách mua thành công:", r.scard("buyers:g1"))

# Phiên bản lỗi (check-then-act) để quan sát hiện tượng bán lố (overselling):
def buy_general_naive(event, user):
    if int(r.get(f"stock:{event}") or 0) > 0:
        r.decr(f"stock:{event}")
        r.sadd(f"buyers:{event}", user)
        return True
    return False

r.flushdb()
r.set("stock:g1", 100)
ok_naive = race(lambda i: buy_general_naive("g1", f"user {i}"), n=500)
print("Bán lố với buy_general_naive - Vé bán ra:", sum(ok_naive), "| Kho còn lại:", r.get("stock:g1"))

# Step 4: Rate limiting - Fixed window
def allow_fixed(user, limit=5, window=10):
    key = f"rl:fixed:{user}:{int(time.time() // window)}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window, nx=True)  # Chỉ đặt TTL ở lần đầu tiên
    count, _ = pipe.execute()
    return count <= limit

print("Fixed window (8 requests):", [allow_fixed("linh") for _ in range(8)])

# Thí nghiệm ranh giới cửa sổ với Fixed window (10 requests trong ~0.4s):
wait_fixed = 10 - (time.time() % 10) - 0.2
time.sleep(wait_fixed if wait_fixed >= 0 else wait_fixed + 10)
first_fixed = [allow_fixed("bot_fixed") for _ in range(5)]
time.sleep(0.4)
second_fixed = [allow_fixed("bot_fixed") for _ in range(5)]
print("Fixed window boundary burst:", first_fixed + second_fixed)

SLIDING = r.register_script("""
local now, window, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
if redis.call('ZCARD', KEYS[1]) < limit then
    redis.call('ZADD', KEYS[1], now, ARGV[4])
    redis.call('PEXPIRE', KEYS[1], window)
    return 1
end
return 0
""")

def allow_sliding(user, limit=5, window_ms=10_000):
    now = int(time.time() * 1000)
    return bool(SLIDING(keys=[f"rl:sliding:{user}"], args=[now, window_ms, limit, uuid.uuid4().hex]))

def try_hold(event, seat, user):
    if not allow_sliding(user):
        return "rate_limited"
    return "ok" if hold(event, seat, user) else "taken"

# Thí nghiệm ranh giới cửa sổ với Sliding window (lô thứ 2 bị từ chối):
wait_sliding = 10 - (time.time() % 10) - 0.2
time.sleep(wait_sliding if wait_sliding >= 0 else wait_sliding + 10)
first_sliding = [allow_sliding("bot_sliding") for _ in range(5)]
time.sleep(0.4)
second_sliding = [allow_sliding("bot_sliding") for _ in range(5)]
print("Sliding window boundary:", first_sliding + second_sliding)
print("try_hold test:", [try_hold("c1", f"B{i}", "linh_rl") for i in range(6)])

def join_queue(event, user):
    r.zadd(f"queue:{event}", {user: time.time()}, nx=True)
    return r.zrank(f"queue:{event}", user) + 1  # Trả về thứ tự hàng chờ

def admit(event, n):
    return [user for user, _ in r.zpopmin(f"queue:{event}", n)]  # Mời n người vào mua vé

for name in ["u1", "u2", "u3"]:
    print(name, join_queue("c1", name))   # positions 1, 2, 3
print(join_queue("c1", "u1"))             # still 1
print(admit("c1", 2))                     # ['u1', 'u2']
print(join_queue("c1", "u3"))             # 1

SEATS = [f"S{i}" for i in range(1, 21)]

def buyer(i):
    user = f"user {i}"
    for seat in SEATS:
        if hold("c1", seat, user):
            time.sleep(random.random() * 0.05)  # Giả lập thời gian thanh toán
            if confirm("c1", seat, user):
                return seat
    return None

for run in range(1, 11):
    r.flushdb()
    got = [s for s in race(buyer, n=100) if s]
    print(f"Lần {run}: Vé bán ra: {len(got)} | Ghế duy nhất: {len(set(got))} | Đã ghi nhận trong Redis: {r.scard('sold:c1')}")
# Kết quả cả 3 số ở cả 10 lần phải bằng 20 (20 - 20 - 20)