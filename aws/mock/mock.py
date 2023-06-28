import random
import numpy as np
import string
import time
import sys
import logging
from kafka import KafkaProducer
import multiprocessing as mp
import signal

KAFKA_TOPIC = 'sensor-data'

logger = logging.getLogger('kafka')

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

def calc_speed(car):
    return car.speed

def car_plate():
    # cria a placa do carro
    letters = ''.join(random.choices(string.ascii_uppercase, k=2))
    numbers = ''.join(random.choices(string.digits, k=3))
    plate = letters + numbers
    return plate

def send_message(road_name, road_size, road_lanes, road_speed, car, mode):
    producer = KafkaProducer()
    # with open("all_roads.csv", "a") as f:
    #     if mode == "forward":
    #         f.write(str(road_name) + "," + str(road_speed) + "," + str(road_size) + "," + str(car.x) + "," + str(car.y) + "," + str(car.plate) + "," + str(time.time()) + "," + "1" + "\n")
    #     else:
    #         f.write(str(road_name) + "," + str(road_speed) + "," + str(road_size) + "," + str(road_size - car.x) + "," + str(car.y + road_lanes) + "," + str(car.plate) + "," + str(time.time()) + "," + "-1" + "\n")

    if mode == "forward":
        message = str(road_name) + "," + str(road_speed) + "," + str(road_size) + "," + str(car.x) + "," + str(car.y) + "," + str(car.plate) + "," + str(time.time()) + "," + "1" + "\n"
    else:
        message = str(road_name) + "," + str(road_speed) + "," + str(road_size) + "," + str(road_size - car.x) + "," + str(car.y + road_lanes) + "," + str(car.plate) + "," + str(time.time()) + "," + "-1" + "\n"
    producer.send(KAFKA_TOPIC, bytes(message, 'utf-8'))
    producer.close()

def sub(road, mode):
    global processes_cars
    processes_cars = []

    # cria a matriz que representa a estrada
    matrix_cars = np.full((road.size,road.lanes), "XXXXXX")

    cars = road.vehicles
    cars.sort(key = calc_speed) # ordena os carros por velocidade

    for car in cars:
        # atualiza o contador de ciclos para remover a colisão
        if car.collision:
            car.cicles_to_remove_collision += 1
            if car.cicles_to_remove_collision == road.cicles_to_remove_collision:
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
            # enviar mensagem aqui
            send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

            car.collision = True
            # define que o carro em que ele bateu também colidiu
            for car_2 in cars:
                if car_2.plate == plate_colided:
                    car_2.collision = True
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

        # se nao houver, só atualiza a posicao do carro
        if not trigger_collision:
            matrix_cars[car.x + car.speed][car.y] = car.plate
            car.x += car.speed
            # enviar mensagem aqui
            send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

        # checa a possibilidade de o carro trocar de pista
        # adicionamos car.collision == False para evitar que o carro troque de pista se ja colidiu
        if trigger_collision and car.collision == False:
            # se conseguiu trocar de pista, nao ha mais risco de colisao
            if car.y != 0: # para a esquerda
                if matrix_cars[car.x + car.speed][car.y - 1] == "XXXXXX":
                    trigger_collision = False
                    matrix_cars[car.x + car.speed][car.y - 1] = car.plate
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)


            if car.y != road.lanes-1: # para a direita
                if matrix_cars[car.x + car.speed][car.y + 1] == "XXXXXX": #and trigger_collision:
                    trigger_collision = False
                    matrix_cars[car.x + car.speed][car.y + 1] = car.plate
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

                
            # se nao conseguiu trocar de pista, diminui a velocidade
            if trigger_collision:
                if car.speed - (achieved_speed+1) < road.max_decceleration:
                    car.speed -= (achieved_speed+1)
                    matrix_cars[car.x + car.speed][car.y] = car.plate
                    trigger_collision = False
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)
                    
        # se nao conseguiu trocar de pista ou diminuir a velocidade, colidiu
        if trigger_collision:
            car.x = collision_pos
            car.collision = True
            # enviar mensagem aqui
            send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

            # define que o carro em que ele bateu também colidiu
            for car_2 in cars:
                if car_2.plate == plate_colided:
                    car_2.collision = True
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

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
                    car.x += car.speed
                elif car.y == 0:
                    car.y += 1
                    car.x += car.speed
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)

                else:
                    car.y -= 1
                    # enviar mensagem aqui
                    send_message(road.name, road.size, road.lanes, road.max_speed, car, mode)
    
    for p_car in processes_cars:
        p_car.join()

    for i in range(road.lanes):
        if random.random() < road.prob_vehicle_surge:
            plate = car_plate()
            # carros entram com uma velocidade entre o mínimo e o máximo da pista
            car = vehicle(0, i, plate, random.randint(road.min_speed, road.max_speed))
            cars.append(car)
    road.vehicles = cars

def signal_handler(signal, frame):
    print("Keyboard interrupt received. Terminating all processes...")
    for p in processes:
        p.terminate()
    sys.exit(0)

def simulate_road(road_fwd, road_bwd):
    while True:
        sub(road_fwd, "forward")
        sub(road_bwd, "backward")

def main(num_instances):
    global processes
    processes = []
    i = 0

    while i < num_instances:
        #road_number = random.randint(1, 31)
        # road_name = random.choice(["de janeiro", "de fevereiro", "de março", "de abril", "de maio", "de junho", "de julho", "de agosto", "de setembro", "de outubro", "de novembro", "de dezembro"])
        #road_name = random.choice(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sept", "oct", "nov", "dec"])
        road_speed_limit = random.randint(120, 200)
        road_size = random.randint(50000, 100000)
        road_lanes = random.randint(2, 4)

        # call the function to create the road
        road_fwd = road("road" + str(i), road_lanes, road_size, 3, .5, .1, road_speed_limit, 60, .2, 5, 2, 200)
        road_bwd = road("road" + str(i), road_lanes, road_size, 3, .5, .1, road_speed_limit, 60, .2, 5, 2, 200)

        p = mp.Process(target=simulate_road, args=(road_fwd, road_bwd))
        p.start()
        processes.append(p)
        i += 1
        
    for p in processes:
        p.join()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    num_instances = int(input("Enter the number of instances: "))
    main(num_instances)