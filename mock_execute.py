def execute(command: list[str]) -> dict:
    """
    실제 redis.py가 아직 없을 때 서버 연결 흐름만 점검하기 위한 임시 execute 함수이다.

    이 함수의 목적은 "명령을 제대로 처리한다"가 아니라,
    서버가 다음 순서로 잘 이어지는지만 확인하는 것이다.
    1. 소켓으로 문자열을 받는다.
    2. parser가 list[str]로 바꾼다.
    3. server가 execute()를 호출한다.
    4. execute() 결과를 응답 문자열로 다시 보낸다.
    """

    # 최소한의 방어 코드이다.
    # parser 단계에서 이미 빈 명령은 막히지만, execute 쪽도 입력이 비정상이면
    # 어떤 문제가 생겼는지 바로 보이도록 에러 응답을 돌려준다.
    if not command:
        return {"type": "error", "value": "empty command"}

    # 명령어는 parser에서 대문자로 정규화되지만,
    # 이 함수만 따로 호출될 수도 있어서 한 번 더 안전하게 처리한다.
    name = command[0].upper()

    # PING은 서버와 execute 연결 상태를 확인하기 가장 쉬운 명령이다.
    # 요청이 여기까지 도달했다는 뜻이므로 PONG을 돌려준다.
    if name == "PING":
        return {"type": "simple_string", "value": "PONG"}

    # ECHO는 문자열 인자가 execute까지 그대로 들어오는지 보기 좋다.
    # 인자 개수가 맞지 않으면 에러를 돌려준다.
    if name == "ECHO":
        if len(command) != 2:
            return {"type": "error", "value": "wrong number of arguments"}
        return {"type": "bulk_string", "value": command[1]}

    # 그 외 명령은 아직 임시 execute 범위 밖이므로,
    # "서버는 연결됐지만 코어 기능은 아직 구현 전"이라는 의미의 에러를 돌려준다.
    return {"type": "error", "value": "mock execute supports only PING and ECHO"}
