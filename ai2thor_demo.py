from ai2thor.controller import Controller

from ai2thor.platform import CloudRendering
controller = Controller(platform=CloudRendering)
controller.start()
controller.reset('FloorPlan1')

from PIL import Image
# 保存第一视角图片
image = Image.fromarray(controller.last_event.frame)
# current_path = os.getcwd()
# full_path = os.path.join(current_path, path)
# full_path = os.path.normpath(full_path)
image.save('first_person_view.png')
# 关闭控制器
controller.stop()