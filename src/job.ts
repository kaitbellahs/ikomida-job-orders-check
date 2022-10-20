import { Utils, DBModels, Types, Domain, GateWays } from '@ikomida/shared-backend'
import { createRequire } from 'module'
const require = createRequire(import.meta.url)
let { name } = require('../package.json')
name = name
  .replace(/^(@\S+\/)?(svelte-)?(\S+)/, '$3')
  .replace(/^\w/, (m: string) => m.toUpperCase())
  .replace(/-\w/g, (m: string[]) => m[1].toUpperCase())

class OrdersCheckJob {
  logger

  constructor() {
    this.logger = Utils.Logger.getInstance(name)
  }

  async run() {
    try {
      this.logger.error(`Orders checker started...!`)
      const orders = await DBModels.OrderModel.findAll({
        order: [['createdAt', 'DESC']],
        where: {
          status: {
            [Domain.SqlDB.Op.in]: [Types.Types.TOrderStatus.WAITING_PAYMENT, Types.Types.TOrderStatus.OPEN]
          },
          createdAt: {
            [Domain.SqlDB.Op.gt]: new Date(new Date().getTime() - 15 * 60 * 1000)
          }
        },
        include: [
          {
            model: DBModels.ContractModel,
            required: false
          },
          {
            model: DBModels.UserPaymentModel,
            required: false
          },
          {
            model: DBModels.UserModel,
            required: true,
            include: [
              {
                model: DBModels.PNModel,
                required: false
              }
            ]
          }
        ]
      })
      const status = Types.Types.TOrderStatus.CANCELED
      for (const order of orders) {
        try {
          order.status = status
          if (
            order.userPayment &&
            order.userPayment?.status !== Types.Types.TPagSeguroPaymentStatus.CANCELED &&
            order.paymentMethodType === Types.Types.TPaymentMethod.CREDIT_CARD_ONLINE
          ) {
            try {
              const paymentPayload = new Types.Classes.CAMQPPayload<string>({
                method: 'cancelPayment',
                object: order.userPayment?.id
              })
              const amqp = new Domain.RabbitMQ(this.logger)
              await amqp?.publish(Domain.RabbitMQ.PAYMENT_QUEUE, paymentPayload)
              await amqp?.close()
            } catch (exception: any) {
              new Utils.iKomidaError(
                Utils.iKomidaError.IKOMIDA_ORDERS_SERVICE_CHANGE_ORDER_STATUS_PAYMENT_EXCEPTION,
                exception?.message,
                exception
              ).log(this.logger)
              throw new Utils.iKomidaError(Utils.iKomidaError.IKOMIDA_ORDERS_SERVICE_CHANGE_ORDER_STATUS_ERROR)
            }
          }
          await order.save()
          try {
            const pNModel = order.user?.pN
            if (pNModel) {
              await Utils.Notification.sendNotification(
                this.logger,
                Utils.Notification.USER_ORDER_UPDATED,
                order?.id,
                order.contract?.id,
                order?.user?.id,
                status.name
              )
            }
            await Utils.Notification.sendNotification(
              this.logger,
              Utils.Notification.VENDOR_ORDER_UPDATED,
              order?.id,
              order.contract?.id,
              undefined,
              order?.customID,
              status.name
            )
          } catch (exception: any) {
            const error = new Utils.iKomidaError(
              Utils.iKomidaError.IKOMIDA_ORDERS_SERVICE_NEW_ORDER_PRODUCTS_EXCEPTION,
              exception?.message
            )
            error.log(this.logger)
          }
        } catch (exception) {
          this.logger.error(exception)
        }
      }
    } catch (exception) {
      this.logger.error(exception)
    }
  }
}

await new OrdersCheckJob().run()
