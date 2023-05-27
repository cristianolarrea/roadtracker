import random
import numpy as np
import string
import time
import multiprocessing as mp
import json
#import pickle
import grpc
import rpc_pb2
import rpc_pb2_grpc
import signal
import sys

# carol 192.168.137.1
# cris 10.42.0.1
SERVER_IP = '127.0.0.1'
SERVER_PORT = '50051'

class vehicle:
    def __init__(self, x, y, plate, speed):
        self.x = x
        self.y = y
        self.plate = plate
        self.speed = speed
        self.acceleration = 0

        self.collision = False
        self.cicles_to_remove_collision = 0

class road:
    def __init__(self, name, lanes, size, cicles_to_remove_collision,
                 prob_vehicle_surge, prob_lane_change, max_speed, min_speed, collision_risk,
                 max_acceleration, max_decceleration, speed_limit):
        self.name = name
        self.lanes = lanes
        self.size = size
        self.speed_limit = speed_limit # limite físico para qualquer carro
        self.prob_vehicle_surge = prob_vehicle_surge
        self.prob_lane_change = prob_lane_change
        self.max_speed = max_speed
        self.min_speed = min_speed
        self.cicles_to_remove_collision = cicles_to_remove_collision
        self.collision_risk = collision_risk
        self.max_acceleration = max_acceleration
        self.max_decceleration = max_decceleration # positivo 

        self.vehicles = []
        self.vehicles_to_remove = False

def write_to_string(name, max_speed, list_cars, timestamp, mode, lanes, size):
    road_info = {}
    road_info["time"] = timestamp
    road_info["avenue"] = name + "_" + str(max_speed)
    road_info["cars"] = {}

    for car in list_cars:
        if mode == "forward":
            road_info["cars"][car.plate] = "(" + str(car.x) + "," + str(car.y) + ")"
        elif mode == "backward":
            road_info["cars"][car.plate] = "(" + str(size - car.x) + "," + str(lanes + car.y) + ")"

    return road_info

def calc_speed(car):
    return car.speed

def car_plate():
    # cria a placa do carro
    letters = ''.join(random.choices(string.ascii_uppercase, k=2))
    numbers = ''.join(random.choices(string.digits, k=3))
    plate = letters + numbers
    return plate

def sub(road, mode):
    # cria a matriz que representa a estrada
    matrix_cars = np.full((road.size,road.lanes), "XXXXXX")

    cars = road.vehicles
    cars.sort(key = calc_speed) # ordena os carros por velocidade

    for car in cars:
        print(car.x)
        # atualiza o contador de ciclos para remover a colisão
        if car.collision:
            car.cicles_to_remove_collision += 1
            if car.cicles_to_remove_collision == road.cicles_to_remove_collision:
                print("Removed", car.plate)
                cars.remove(car)
                continue

        trigger_collision = False

        for i in range(car.speed):
            # checa se há carros no meio do avanço do carro
            achieved_speed = i
            if matrix_cars[car.x + i][car.y] != "XXXXXX":
                trigger_collision = True
                collision_pos = car.x + i
                plate_colided = matrix_cars[car.x + i][car.y]
                break

        # forçar colisão se a probabilidade de colisão for maior que o random
        if random.random() < road.collision_risk and trigger_collision:
            car.x = collision_pos
            car.collision = True
            # define que o carro em que ele bateu também colidiu
            for car_2 in cars:
                if car_2.plate == plate_colided:
                    car_2.collision = True

        # se nao houver, só atualiza a posicao do carro
        if not trigger_collision:
            matrix_cars[car.x + car.speed][car.y] = car.plate
            car.x += car.speed

        # checa a possibilidade de o carro trocar de pista
        # adicionamos car.collision == False para evitar que o carro troque de pista se ja colidiu
        if trigger_collision and car.collision == False:
            # se conseguiu trocar de pista, nao ha mais risco de colisao
            if car.y != 0: # para a esquerda
                if matrix_cars[collision_pos][car.y - 1] != "XXXXXX":
                    trigger_collision = False
                    matrix_cars[collision_pos][car.y - 1] = car.plate

            if car.y != road.lanes-1: # para a direita
                if matrix_cars[collision_pos][car.y + 1] != "XXXXXX": #and trigger_collision:
                    trigger_collision = False
                    matrix_cars[collision_pos][car.y + 1] = car.plate

            # se nao conseguiu trocar de pista, diminui a velocidade
            if trigger_collision:
                if car.speed - (achieved_speed+1) < road.max_decceleration:
                    car.speed -= (achieved_speed+1)
                    matrix_cars[car.x + car.speed][car.y] = car.plate
                    trigger_collision = False

        # se nao conseguiu trocar de pista ou diminuir a velocidade, colidiu
        if trigger_collision:
            car.x = collision_pos
            car.collision = True
            print("Colided", car.plate)
            # define que o carro em que ele bateu também colidiu
            for car_2 in cars:
                if car_2.plate == plate_colided:
                    car_2.collision = True

        if car.collision == True:
            car.speed = 0
        else:
            # define velocidade e aceleração do carro
            if car.speed == 0:
                car.speed = road.max_decceleration
            else:
                car.speed += car.acceleration

            accel_new = random.randrange(-road.max_acceleration,road.max_acceleration)

            # mantém a velocidade dos carros em movimento acima do limite minimo
            if car.speed + accel_new < road.min_speed:
                car.acceleration = road.max_decceleration - 1
            # ou evita que a velocidade do carro ultrapasse o limite físico do carro
            elif car.speed + accel_new > road.speed_limit:
                car.speed = road.speed_limit
            # ou atualiza a aceleração
            else:
                car.acceleration = accel_new

            # tira o carro da pista caso sua posição seja maior que o tamanho da pista
            if car.x + car.speed > road.size-1:
                cars.remove(car)

        # checa se o carro vai trocar de pista
        if road.prob_lane_change > random.random():
            if car.speed > 0:
                if car.y != 0 and car.y != road.lanes -1:
                    car.y = random.choice([car.y+1,car.y-1])
                elif car.y == 0:
                    car.y += 1
                else:
                    car.y -= 1

    for i in range(road.lanes):
        if random.random() < road.prob_vehicle_surge:
            plate = car_plate()
            # carros entram com uma velocidade entre o mínimo e o máximo da pista
            car = vehicle(0, i, plate, random.randint(road.min_speed, road.max_speed))
            cars.append(car)
    road.vehicles = cars

    time.sleep(0.05)

def signal_handler(signal, frame):
    print("Keyboard interrupt received. Terminating all processes...")
    for p in processes:
        p.terminate()
    sys.exit(0)

def simulate_road(road_fwd, road_bwd):
    channel = grpc.insecure_channel(SERVER_IP + ':' + SERVER_PORT)
    stub = rpc_pb2_grpc.RoadSimStub(channel)
    
    last_request_name = None 
    
    while True:
        tempo = int(1000*time.time())
        tempo = str(tempo)[-9:]
        sub(road_fwd, "forward")
        stringForward = write_to_string(road_fwd.name, road_fwd.max_speed, road_fwd.vehicles, tempo, "forward", road_fwd.lanes, road_fwd.size)
        sub(road_bwd, "backward")
        stringBackward = write_to_string(road_bwd.name, road_bwd.max_speed, road_bwd.vehicles, tempo, "backward", road_bwd.lanes, road_bwd.size)
        stringForward['cars'].update(stringBackward['cars'])
        json_string = json.dumps(stringForward)
        #pickle.dump(json_string, open("json_string.p", "wb"))

        try:
            # change to ..\json_string.p if your o.s. is Windows
            # ../json_string.p if your o.s. is Linux or Mac
            request = rpc_pb2.Request(name=json_string)
            request_name = request.name

            # checks if the last message is different from the current one
            if request_name != last_request_name:
                # sends the message to the server if it is different
                response = stub.Simulate(request)
                last_request_name = request_name
        except:
            continue


def main(num_instances):
    global processes
    processes = []
    i = 0
    total_ciclos = 10000
    
    while i < num_instances:
        for ciclo in range(total_ciclos):
            for i in range(num_instances):
                time.sleep(0.05)
                road_fwd = road("road" + str(i), 2, 150000, 5, .5, .1, 120, 60, .2, 5, 2,200)
                road_bwd = road("road" + str(i), 2, 150000, 5, .5, .1, 120, 60, .2, 5, 2,200)
                p = mp.Process(target=simulate_road, args=(road_fwd, road_bwd))
                p.start()
                processes.append(p)
            
            for p in processes:
                p.join()
        
        i = i + 1
        
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    num_instances = int(input("Enter the number of instances: "))
    main(num_instances)