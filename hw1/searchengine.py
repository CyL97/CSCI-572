import cv2
import os
import sys
import time


def get_img(input_dir):
    img_paths = []
    for (path,dirname,filenames) in os.walk(input_dir):
        for filename in filenames:
            if filename[-1] == 'g':
                img_paths.append(path+'/'+filename)
    print("img_paths:",img_paths)
    return img_paths


def cut_img(img_paths,output_dir):
    scale = len(img_paths)
    for i,img_path in enumerate(img_paths):
        a = "#"* int(i/1000)
        b = "."*(int(scale/1000)-int(i/1000))
        c = (i/scale)*100
        time.sleep(0.2)
        print('正在处理图像： %s' % img_path.split('/')[-1])
        img = cv2.imread(img_path)
        weight = img.shape[1]
        if weight>1600:                         # 正常发票
            cropImg = img[50:200, 700:1500]    # 裁剪【y1,y2：x1,x2】
            #cropImg = cv2.resize(cropImg, None, fx=0.5, fy=0.5,
                                 #interpolation=cv2.INTER_CUBIC) #缩小图像
            cv2.imwrite(output_dir + '/' + img_path.split('/')[-1], cropImg)
        else:                                        # 卷帘发票
            cropImg_01 = img[250:700, 290:890]
            cv2.imwrite(output_dir + '/'+img_path.split('/')[-1], cropImg_01)
        print('{:^3.3f}%[{}>>{}]'.format(c,a,b))

if __name__ == '__main__':
    output_dir = "/Users/lichaoyu/Desktop/tmep"           # 保存截取的图像目录
    input_dir = "/Users/lichaoyu/Desktop/bb_psp_img_1"                # 读取图片目录表
    img_paths = get_img(input_dir)
    print('图片获取完成 。。。！')
    cut_img(img_paths,output_dir)