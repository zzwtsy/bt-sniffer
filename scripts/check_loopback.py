"""CI 的双栈覆盖前提：实际绑定 TCP/UDP，退出 with 后释放 socket。"""
import socket


def check_loopback():
    for family, host in [(socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")]:
        for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
            with socket.socket(family, kind) as connection:
                connection.bind((host, 0))
                if kind == socket.SOCK_STREAM:
                    connection.listen(1)
            print(f"loopback 可用：{host} {kind.name}")


if __name__ == "__main__":
    try:
        check_loopback()
    except OSError as error:
        print(f"环境阻塞：双栈 loopback 不可用：{error}")
        raise SystemExit(2)
