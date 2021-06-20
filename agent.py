import random

class Agent:
    # INITIALIZATION
    def __init__(self):
        # PROPETIES
        self.device_id = 0
        self.pressure = 100
        self.last_remembered_pressure = 0
        self.power_state = True
        self.pressure_limit = 150

        # ERROR PRIMITIVES
        self.error_0 = False    # unidentified error
        self.error_1 = False    # no water intake
        self.error_2 = False    # no power intake


    # ==========================================================
    # DIRECT METHODS

    # Flips the switch and saves pressure of water
    def pump_switch(self):
        # Turning OFF
        if self.power_state:
            self.last_remembered_pressure = self.pressure
            self.pressure = 0
            self.power_state = False
            print('Power OFF.')
            return
        
        # Turning ON
        if not self.power_state:
            self.pressure = self.last_remembered_pressure
            self.last_remembered_pressure = -1
            self.power_state = True
            print('Power ON.')
            return


    # Resets all the alarms
    def alarm_reset(self):
        self.error_0 = False    # unidentified error
        self.error_1 = False    # no water intake
        self.error_2 = False    # no power intake

        print('All errors are now reseted. Peace...')

    def raise_error(self, error_nr = int):
        if error_nr < 0 or error_nr > 2:
            print('No such error in existance.')
            return

        print('Raising ERROR_' + str(error_nr))
        
        if error_nr == 0:
            self.error_0 = True
            return
        if error_nr == 1:
            self.error_1 = True
            return

        self.error_2 = True
    # ==========================================================
    # DEVICE TWIN METHODS

    # Sets the pressure of water pump
    def set_pressure(self, desired_pressure:float):
        # In case of negative desired pressure
        if desired_pressure < 0:
            self.pressure = 0
        
        
        self.pressure = desired_pressure
        print("Pressure set to: " + str(self.pressure))

    def set_device_id(self, id:int):
        self.device_id = id

    # Gets the pressure of water pump
    def get_pressure(self):
        return self.pressure + random.gauss(0, 10)

    # Gets whether the alarm should be raised
    def get_alarm_state(self):
        # If any of the errors was detected - raise the alarm
        if self.error_0 is True or self.error_1 is True or self.error_2 is True:
            return True
        # Otherwise don't
        return False

    # Gets whether the alarm should be raised in int
    def get_alarm_state_int(self):
        # If any of the errors was detected - raise the alarm
        if self.error_0 is True or self.error_1 is True or self.error_2 is True:
            return 1
        # Otherwise don't
        return 0
    
    def buisness_logic(self):
        print('Alarm state noticed - ')
        self.pressure = 0
        self.state = 0

    # Returns codes of raised alarms
    def get_alarm_codes(self):
        codes = set()

        if self.error_0:
            codes.append(0)
        if self.error_1:
            codes.append(1)
        if self.error_2:
            codes.append(2)

        return codes

    def get_error_0_to_int(self):
        if self.error_0:
            return 1
        return 0

    def get_error_1_to_int(self):
        if self.error_1:
            return 1
        return 0

    def get_error_2_to_int(self):
        if self.error_2:
            return 1
        return 0

    def get_errors_int(self):
        errors = list()

        if self.error_0:
            errors.append(1)
        else:
            errors.append(0)

        if self.error_1:
            errors.append(1)
        else:
            errors.append(0)

        if self.error_2:
            errors.append(1)
        else:
            errors.append(0)

        print('errors:' + str(errors))
        return errors
    
    # REPORTING METHODS FOR DEVICE TWIN

    #def report(self):


    
        



    
    







# Elegant

if __name__ == "__main__":
    print('agent.py - Running')
else:
    print('agent.py - Imported externally')